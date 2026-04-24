const REGION_SYSTEMS = {
  'emilia-romagna': 'ross1000',
  'marche': 'istrice_ross1000',
  'veneto': 'ross1000',
  'valle-daosta': 'vit_albergatori',
};

const REGION_DEFAULTS = {
  'emilia-romagna': {
    sistema: 'ross1000',
    portal_url: null,
    deadline_rule: 'entro il 5 del mese successivo',
    requires_open_close: false,
    supports_file_import: false,
    supports_webservice: false,
    export_format: 'json',
  },
  'marche': {
    sistema: 'istrice_ross1000',
    portal_url: null,
    deadline_rule: 'entro il 5 del mese successivo',
    requires_open_close: true,
    supports_file_import: true,
    supports_webservice: false,
    export_format: 'json',
  },
  'veneto': {
    sistema: 'ross1000',
    portal_url: null,
    deadline_rule: 'entro i primi 10 giorni lavorativi del mese successivo',
    requires_open_close: false,
    supports_file_import: false,
    supports_webservice: false,
    export_format: 'json',
  },
  'valle-daosta': {
    sistema: 'vit_albergatori',
    portal_url: null,
    deadline_rule: 'entro il 5 del mese successivo',
    requires_open_close: true,
    supports_file_import: false,
    supports_webservice: false,
    export_format: 'json',
  },
};

const REGION_BY_PROVINCE = {
  AO: 'valle-daosta',
  VE: 'veneto',
  VR: 'veneto',
  VI: 'veneto',
  PD: 'veneto',
  TV: 'veneto',
  BL: 'veneto',
  RO: 'veneto',
  AN: 'marche',
  AP: 'marche',
  FM: 'marche',
  MC: 'marche',
  PU: 'marche',
  RN: 'emilia-romagna',
  FC: 'emilia-romagna',
  BO: 'emilia-romagna',
  FE: 'emilia-romagna',
  MO: 'emilia-romagna',
  PR: 'emilia-romagna',
  PC: 'emilia-romagna',
  RA: 'emilia-romagna',
  RE: 'emilia-romagna',
};

const STOPWORDS = new Set([
  'appartamento', 'appartamenti', 'apt', 'app', 'appto', 'appto', 'apartment',
  'residence', 'residenza', 'casa', 'house', 'suite', 'room', 'il', 'lo', 'la',
  'di', 'del', 'della', 'the', 'via', 'viale', 'corso', 'piazza', 'vicolo',
  'lungomare'
]);

function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\bapp\.?to\b/g, ' appartamento ')
    .replace(/\bv\.le\b/g, ' viale ')
    .replace(/\bc\.so\b/g, ' corso ')
    .replace(/\bp\.zza\b/g, ' piazza ')
    .replace(/\bv\.lo\b/g, ' vicolo ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function slugify(value) {
  return normalizeText(value).replace(/\s+/g, '-');
}

function compact(value) {
  return normalizeText(value).replace(/\s+/g, '');
}

function unique(list) {
  return Array.from(new Set((list || []).filter(Boolean)));
}

function toTokens(value) {
  return normalizeText(value)
    .split(' ')
    .map(token => token.trim())
    .filter(token => token && !STOPWORDS.has(token) && token.length > 1);
}

function extractStreetKeys(apartment) {
  const sources = [apartment.indirizzo_completo, apartment.nome_appartamento];
  const keys = [];
  const regex = /\b(via|viale|corso|piazza|vicolo|lungomare)\s+[a-z0-9 ]+?\s+\d+[a-z0-9\/-]*/g;
  sources.forEach(source => {
    const normalized = normalizeText(source);
    const matches = normalized.match(regex) || [];
    matches.forEach(match => keys.push(match.trim()));
  });
  return unique(keys);
}

function extractLocationKeys(apartment) {
  const keys = [];
  if (apartment.struttura_nome) keys.push(normalizeText(apartment.struttura_nome));
  if (apartment.provincia) keys.push(normalizeText(apartment.provincia));
  const name = normalizeText(apartment.nome_appartamento);
  const parts = name.split(' ').filter(Boolean);
  if (parts.length) keys.push(parts[parts.length - 1]);
  return unique(keys).filter(Boolean);
}

function derivePublicCheckinBase(apartment) {
  const codeSlug = slugify(apartment.codice_interno || '');
  const nameSlug = slugify(apartment.nome_appartamento || '');
  if (codeSlug) {
    const normalizedCode = normalizeText(apartment.codice_interno || '');
    const nameWithoutCode = normalizeText(apartment.nome_appartamento || '')
      .replace(new RegExp(`^${escapeRegex(normalizedCode)}\\s*`, 'i'), '')
      .replace(/^[- ]+/, '')
      .trim();
    const nameRemainderSlug = slugify(nameWithoutCode);
    return nameRemainderSlug && !nameRemainderSlug.startsWith(codeSlug)
      ? `${codeSlug}-${nameRemainderSlug}`
      : codeSlug;
  }
  return nameSlug || 'appartamento';
}

function buildUniquePublicCheckinKey(apartment, usedKeys) {
  const base = derivePublicCheckinBase(apartment).replace(/^-+|-+$/g, '').slice(0, 64) || 'appartamento';
  let candidate = base;
  let suffix = 2;
  while (usedKeys.has(candidate.toLowerCase())) {
    candidate = `${base.slice(0, Math.max(1, 60 - String(suffix).length))}-${suffix}`;
    suffix += 1;
  }
  return candidate;
}

function classifyPublicCheckinKey(apartment, duplicateKeys) {
  const raw = String(apartment.public_checkin_key || '').trim();
  if (!raw) return { status: 'MISSING', notes: [] };
  const key = raw.toLowerCase();
  const notes = [];
  if (duplicateKeys.has(key)) return { status: 'DUPLICATE_SUSPECT', notes: ['Chiave duplicata case-insensitive'] };
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(raw)) notes.push('Formato chiave non pulito');
  return { status: 'PRESENT', notes };
}

function scoreAccountMatch(apartment, account, channelHints = []) {
  const accountText = normalizeText([account.nome_account, account.questura].filter(Boolean).join(' '));
  const compactAccountText = compact(accountText);
  const apartmentName = normalizeText(apartment.nome_appartamento);
  const apartmentCode = normalizeText(apartment.codice_interno);
  const apartmentStructure = normalizeText(apartment.struttura_nome);
  const streetKeys = extractStreetKeys(apartment);
  const locationKeys = extractLocationKeys(apartment);
  const apartmentTokens = unique([
    ...toTokens(apartment.nome_appartamento),
    ...toTokens(apartment.struttura_nome),
    ...toTokens(apartment.indirizzo_completo),
  ]).slice(0, 8);

  let score = 0;
  const reasons = [];

  if (apartmentCode && compactAccountText.includes(compact(apartmentCode))) {
    score += 55;
    reasons.push(`codice interno presente in account (${apartment.codice_interno})`);
  }

  if (apartmentName && accountText === apartmentName) {
    score += 95;
    reasons.push('match esatto nome appartamento/account');
  } else if (apartmentName && accountText.includes(apartmentName)) {
    score += 55;
    reasons.push('nome appartamento contenuto nel nome account');
  }

  const streetMatches = streetKeys.filter(key => accountText.includes(key));
  if (streetMatches.length) {
    score += 65;
    reasons.push(`indirizzo riconosciuto: ${streetMatches[0]}`);
  }

  const locationMatches = locationKeys.filter(key => key && accountText.includes(key));
  if (locationMatches.length) {
    score += 15;
    reasons.push(`localita/struttura riconosciuta: ${locationMatches[0]}`);
  }

  const tokenHits = apartmentTokens.filter(token => accountText.includes(token));
  if (tokenHits.length >= 3) {
    score += 20;
    reasons.push(`token condivisi: ${tokenHits.slice(0, 4).join(', ')}`);
  } else if (tokenHits.length === 2) {
    score += 10;
    reasons.push(`token condivisi: ${tokenHits.join(', ')}`);
  }

  channelHints.forEach(hint => {
    const hintText = normalizeText([hint.external_name, hint.external_unit_id].filter(Boolean).join(' '));
    if (hintText && accountText.includes(hintText)) {
      score += 20;
      reasons.push('hint channel mapping coerente');
    }
  });

  const confidence = score >= 90 ? 'HIGH' : score >= 70 ? 'MEDIUM' : score >= 45 ? 'LOW' : null;
  return { score, confidence, reasons };
}

function inferRegionFromApartment(apartment) {
  const province = String(apartment.provincia || '').trim().toUpperCase().slice(0, 2);
  return REGION_BY_PROVINCE[province] || null;
}

function classifyIstatConfig(config, apartment, link) {
  if (!config) {
    return {
      status: 'ASSENTE',
      notes: [],
      inferred_region: inferRegionFromApartment(apartment),
      override_code: cleanNullable(link?.istat_codice_struttura_override),
    };
  }

  const notes = [];
  const inferredRegion = inferRegionFromApartment(apartment);
  const expectedSystem = config.regione ? REGION_SYSTEMS[config.regione] : null;
  if (config.regione && expectedSystem && config.sistema && config.sistema !== expectedSystem) {
    notes.push(`Sistema incoerente per regione ${config.regione}`);
    return {
      status: 'AMBIGUO',
      notes,
      inferred_region: inferredRegion,
      override_code: cleanNullable(link?.istat_codice_struttura_override),
    };
  }

  if (inferredRegion && config.regione && inferredRegion !== config.regione) {
    notes.push(`Provincia ${apartment.provincia || '—'} suggerisce ${inferredRegion} ma config ha ${config.regione}`);
    return {
      status: 'AMBIGUO',
      notes,
      inferred_region: inferredRegion,
      override_code: cleanNullable(link?.istat_codice_struttura_override),
    };
  }

  const requiredMissing = [];
  if (!config.regione) requiredMissing.push('regione');
  if (!config.sistema) requiredMissing.push('sistema');
  if (!config.codice_struttura) requiredMissing.push('codice_struttura');
  if (config.attivo === false) requiredMissing.push('config disattiva');

  return {
    status: requiredMissing.length ? 'INCOMPLETO' : 'PRESENTE',
    notes: requiredMissing.length ? [`Mancano: ${requiredMissing.join(', ')}`] : notes,
    inferred_region: inferredRegion,
    override_code: cleanNullable(link?.istat_codice_struttura_override),
  };
}

function buildSuggestedActions(row) {
  const actions = [];
  if (row.public_checkin_key_status === 'MISSING') actions.push('CREATE_PUBLIC_CHECKIN_KEY');
  if (row.public_checkin_key_status === 'DUPLICATE_SUSPECT') actions.push('REVIEW_DUPLICATE_PUBLIC_CHECKIN_KEY');
  if (row.alloggiati_status === 'CANDIDATE_HIGH') actions.push('LINK_ALLOGGIATI_HIGH_CONFIDENCE');
  if (row.alloggiati_status === 'AMBIGUOUS') actions.push('REVIEW_ALLOGGIATI_AMBIGUITY');
  if (row.alloggiati_status === 'MISMATCH') actions.push('REVIEW_ALLOGGIATI_MISMATCH');
  if (row.istat_status === 'ASSENTE' && row.can_create_istat) actions.push('CREATE_ISTAT_CONFIG');
  if (row.istat_status === 'INCOMPLETO' && row.can_complete_istat) actions.push('COMPLETE_ISTAT_CONFIG');
  if ((row.istat_status === 'ASSENTE' || row.istat_status === 'INCOMPLETO') && !row.can_create_istat && !row.can_complete_istat) {
    actions.push('REVIEW_ISTAT_MANUALLY');
  }
  if (!actions.length) actions.push('NO_ACTION');
  return actions;
}

function summarizeRows(rows, stats = {}) {
  return {
    apartments_total: rows.length,
    public_checkin_key_present: rows.filter(row => row.public_checkin_key_status === 'PRESENT').length,
    public_checkin_key_created: stats.public_checkin_key_created || 0,
    public_checkin_key_missing: rows.filter(row => row.public_checkin_key_status === 'MISSING').length,
    public_checkin_key_duplicate_suspect: rows.filter(row => row.public_checkin_key_status === 'DUPLICATE_SUSPECT').length,
    alloggiati_connected: rows.filter(row => row.alloggiati_status === 'CONNECTED').length,
    alloggiati_auto_linked: stats.alloggiati_auto_linked || 0,
    alloggiati_ambiguous: rows.filter(row => row.alloggiati_status === 'AMBIGUOUS').length,
    alloggiati_not_linked: rows.filter(row => ['NOT_LINKED', 'CANDIDATE_LOW', 'CANDIDATE_MEDIUM'].includes(row.alloggiati_status)).length,
    istat_present: rows.filter(row => row.istat_status === 'PRESENTE').length,
    istat_created: stats.istat_created || 0,
    istat_incomplete: rows.filter(row => row.istat_status === 'INCOMPLETO').length,
    istat_absent: rows.filter(row => row.istat_status === 'ASSENTE').length,
    errors: stats.errors || 0,
  };
}

function buildAuditRows(context) {
  const duplicateKeys = new Set();
  const keyCounts = new Map();

  context.apartments.forEach(apartment => {
    const key = String(apartment.public_checkin_key || '').trim().toLowerCase();
    if (!key) return;
    keyCounts.set(key, (keyCounts.get(key) || 0) + 1);
  });
  keyCounts.forEach((count, key) => {
    if (count > 1) duplicateKeys.add(key);
  });

  return context.apartments.map(apartment => {
    const currentLink = context.linksByApartmentId.get(apartment.id) || null;
    const currentAccount = currentLink ? context.accountsById.get(currentLink.alloggiati_account_id) || null : null;
    const channelHints = context.channelMappingsByApartmentId.get(apartment.id) || [];
    const publicKeyInfo = classifyPublicCheckinKey(apartment, duplicateKeys);
    const candidates = context.accounts
      .map(account => {
        const scored = scoreAccountMatch(apartment, account, channelHints);
        if (!scored.confidence) return null;
        return {
          account_id: account.id,
          nome_account: account.nome_account,
          questura: account.questura || null,
          confidence: scored.confidence,
          score: scored.score,
          reasons: scored.reasons,
        };
      })
      .filter(Boolean)
      .sort((a, b) => b.score - a.score || String(a.nome_account).localeCompare(String(b.nome_account), 'it'))
      .slice(0, 5);

    let alloggiatiStatus = 'NOT_LINKED';
    const notes = [...publicKeyInfo.notes];
    if (currentLink) {
      if (!currentAccount) {
        alloggiatiStatus = 'MISMATCH';
        notes.push('Mapping Alloggiati presente ma account non trovato');
      } else if (currentAccount.attivo === false) {
        alloggiatiStatus = 'MISMATCH';
        notes.push('Account Alloggiati collegato ma disattivo');
      } else {
        alloggiatiStatus = 'CONNECTED';
      }
    } else if (candidates.length) {
      const top = candidates[0];
      const second = candidates[1];
      const closeSecond = second && Math.abs(top.score - second.score) < 10 && top.confidence === second.confidence;
      if (top.confidence === 'HIGH' && !closeSecond) alloggiatiStatus = 'CANDIDATE_HIGH';
      else if (top.confidence === 'HIGH' || top.confidence === 'MEDIUM') alloggiatiStatus = 'AMBIGUOUS';
      else if (top.confidence === 'MEDIUM') alloggiatiStatus = 'CANDIDATE_MEDIUM';
      else alloggiatiStatus = 'CANDIDATE_LOW';
      if (closeSecond) notes.push('Piu candidati Alloggiati con punteggio vicino');
    }

    const rawIstatRows = context.istatByApartmentId.get(apartment.id) || [];
    const istatConfig = rawIstatRows.length === 1 ? rawIstatRows[0] : null;
    const istatInfo = rawIstatRows.length > 1
      ? { status: 'AMBIGUO', notes: ['Più configurazioni ISTAT trovate'], inferred_region: inferRegionFromApartment(apartment), override_code: cleanNullable(currentLink?.istat_codice_struttura_override) }
      : classifyIstatConfig(istatConfig, apartment, currentLink);
    notes.push(...istatInfo.notes);

    const canCreateIstat = !istatConfig && !!istatInfo.inferred_region && !!istatInfo.override_code;
    const canCompleteIstat = !!istatConfig && istatInfo.status === 'INCOMPLETO' && !!istatInfo.inferred_region && !!istatInfo.override_code;

    const row = {
      apartment_id: apartment.id,
      apartment_label: apartment.nome_appartamento || apartment.codice_interno || apartment.id,
      codice_interno: apartment.codice_interno || null,
      struttura_nome: apartment.struttura_nome || null,
      apartment_name: apartment.nome_appartamento || null,
      current_public_checkin_key: cleanNullable(apartment.public_checkin_key),
      public_checkin_key_status: publicKeyInfo.status,
      alloggiati_status: alloggiatiStatus,
      alloggiati_candidate_matches: candidates,
      current_alloggiati_account_id: currentLink?.alloggiati_account_id || null,
      current_alloggiati_account_label: currentAccount?.nome_account || null,
      current_alloggiati_mapping_id: currentLink?.id || null,
      istat_status: istatInfo.status,
      current_istat_config_id: istatConfig?.id || null,
      current_istat_regione: istatConfig?.regione || null,
      current_istat_codice_struttura: istatConfig?.codice_struttura || null,
      inferred_istat_region: istatInfo.inferred_region || null,
      inferred_istat_system: istatInfo.inferred_region ? REGION_DEFAULTS[istatInfo.inferred_region]?.sistema || null : null,
      istat_codice_struttura_source: istatInfo.override_code || null,
      can_create_istat: canCreateIstat,
      can_complete_istat: canCompleteIstat,
      suggested_action: [],
      notes: unique(notes),
    };

    row.suggested_action = buildSuggestedActions(row);
    return row;
  });
}

function applyAuditRows(context, rows) {
  const usedKeys = new Set(
    context.apartments
      .map(apartment => String(apartment.public_checkin_key || '').trim().toLowerCase())
      .filter(Boolean)
  );

  const plan = rows.map(row => {
    const item = { ...row, actions_taken: [], apply_errors: [] };
    if (row.public_checkin_key_status === 'MISSING') {
      const apartment = context.apartmentsById.get(row.apartment_id);
      const generated = buildUniquePublicCheckinKey(apartment, usedKeys);
      usedKeys.add(generated.toLowerCase());
      item.generated_public_checkin_key = generated;
    }
    return item;
  });

  return plan;
}

async function executeApply(supabase, context, plan, actorEmail) {
  const stats = {
    public_checkin_key_created: 0,
    alloggiati_auto_linked: 0,
    istat_created: 0,
    errors: 0,
  };

  for (const row of plan) {
    try {
      if (row.generated_public_checkin_key) {
        const { error } = await supabase
          .from('apartments')
          .update({ public_checkin_key: row.generated_public_checkin_key })
          .eq('id', row.apartment_id)
          .is('public_checkin_key', null);
        if (error) throw new Error(`update public_checkin_key failed: ${error.message}`);
        row.current_public_checkin_key = row.generated_public_checkin_key;
        row.public_checkin_key_status = 'PRESENT';
        row.actions_taken.push(`PUBLIC_CHECKIN_KEY_CREATED:${row.generated_public_checkin_key}`);
        stats.public_checkin_key_created += 1;
      }

      if (row.alloggiati_status === 'CANDIDATE_HIGH' && !row.current_alloggiati_mapping_id) {
        const candidate = row.alloggiati_candidate_matches[0];
        if (candidate) {
          const payload = {
            apartment_id: row.apartment_id,
            alloggiati_account_id: candidate.account_id,
            id_appartamento_portale: null,
            invio_automatico: false,
          };
          const { error } = await supabase
            .from('apartment_alloggiati')
            .upsert(payload, { onConflict: 'apartment_id' });
          if (error) throw new Error(`upsert apartment_alloggiati failed: ${error.message}`);
          row.current_alloggiati_account_id = candidate.account_id;
          row.current_alloggiati_account_label = candidate.nome_account;
          row.alloggiati_status = 'CONNECTED';
          row.actions_taken.push(`ALLOGGIATI_LINKED:${candidate.nome_account}`);
          stats.alloggiati_auto_linked += 1;
        }
      }

      if (row.istat_status === 'ASSENTE' && row.can_create_istat) {
        const defaults = REGION_DEFAULTS[row.inferred_istat_region];
        const payload = {
          apartment_id: row.apartment_id,
          attivo: true,
          regione: row.inferred_istat_region,
          sistema: defaults.sistema,
          portal_url: defaults.portal_url,
          codice_struttura: row.istat_codice_struttura_source,
          auth_type: null,
          username: null,
          password_encrypted: null,
          supports_file_import: defaults.supports_file_import,
          supports_webservice: defaults.supports_webservice,
          requires_open_close: defaults.requires_open_close,
          deadline_rule: defaults.deadline_rule,
          note: `Creato da sync collegamenti (${actorEmail || 'system'})`,
          export_format: defaults.export_format,
        };
        const { data, error } = await supabase
          .from('apartment_istat_config')
          .insert(payload)
          .select('id')
          .single();
        if (error) throw new Error(`insert apartment_istat_config failed: ${error.message}`);
        row.current_istat_config_id = data.id;
        row.current_istat_regione = row.inferred_istat_region;
        row.current_istat_codice_struttura = row.istat_codice_struttura_source;
        row.istat_status = 'PRESENTE';
        row.actions_taken.push(`ISTAT_CREATED:${row.inferred_istat_region}`);
        stats.istat_created += 1;
      } else if (row.istat_status === 'INCOMPLETO' && row.can_complete_istat && row.current_istat_config_id) {
        const defaults = REGION_DEFAULTS[row.inferred_istat_region];
        const patch = {};
        if (!row.current_istat_regione) patch.regione = row.inferred_istat_region;
        if (!context.istatConfigById.get(row.current_istat_config_id)?.sistema) patch.sistema = defaults.sistema;
        if (!row.current_istat_codice_struttura) patch.codice_struttura = row.istat_codice_struttura_source;
        if (!context.istatConfigById.get(row.current_istat_config_id)?.deadline_rule) patch.deadline_rule = defaults.deadline_rule;
        if (context.istatConfigById.get(row.current_istat_config_id)?.attivo === false) patch.attivo = true;
        if (Object.keys(patch).length) {
          const { error } = await supabase
            .from('apartment_istat_config')
            .update(patch)
            .eq('id', row.current_istat_config_id);
          if (error) throw new Error(`update apartment_istat_config failed: ${error.message}`);
          row.current_istat_regione = patch.regione || row.current_istat_regione;
          row.current_istat_codice_struttura = patch.codice_struttura || row.current_istat_codice_struttura;
          row.istat_status = 'PRESENTE';
          row.actions_taken.push('ISTAT_COMPLETED');
        }
      }
    } catch (err) {
      stats.errors += 1;
      row.apply_errors.push(err.message);
      row.notes = unique([...(row.notes || []), err.message]);
    }
  }

  return { rows: plan, stats };
}

async function loadApartments(supabase) {
  let { data, error } = await supabase
    .from('apartments')
    .select('id,nome_appartamento,codice_interno,struttura_nome,public_checkin_key,attivo,indirizzo_completo,provincia,cap')
    .order('struttura_nome', { ascending: true, nullsFirst: false })
    .order('nome_appartamento', { ascending: true });

  if (error && /(indirizzo_completo|provincia|cap)/i.test(String(error.message || ''))) {
    const retry = await supabase
      .from('apartments')
      .select('id,nome_appartamento,codice_interno,struttura_nome,public_checkin_key,attivo')
      .order('struttura_nome', { ascending: true, nullsFirst: false })
      .order('nome_appartamento', { ascending: true });
    data = retry.data;
    error = retry.error;
  }

  if (error) throw new Error(`load apartments failed: ${error.message}`);
  return data || [];
}

async function loadApartmentAlloggiati(supabase) {
  let { data, error } = await supabase
    .from('apartment_alloggiati')
    .select('id,apartment_id,alloggiati_account_id,id_appartamento_portale,invio_automatico,orario_invio,istat_codice_struttura_override,alloggiati_accounts(id,nome_account,questura,attivo)')
    .order('apartment_id', { ascending: true });

  if (error && /(orario_invio|istat_codice_struttura_override)/i.test(String(error.message || ''))) {
    const retry = await supabase
      .from('apartment_alloggiati')
      .select('id,apartment_id,alloggiati_account_id,id_appartamento_portale,invio_automatico,alloggiati_accounts(id,nome_account,questura,attivo)')
      .order('apartment_id', { ascending: true });
    data = (retry.data || []).map(row => ({
      ...row,
      orario_invio: null,
      istat_codice_struttura_override: null,
    }));
    error = retry.error;
  }

  if (error) throw new Error(`load apartment_alloggiati failed: ${error.message}`);
  return data || [];
}

async function loadAlloggiatiAccounts(supabase) {
  const { data, error } = await supabase
    .from('alloggiati_accounts')
    .select('id,nome_account,questura,attivo')
    .order('nome_account', { ascending: true });

  if (error) throw new Error(`load alloggiati_accounts failed: ${error.message}`);
  return data || [];
}

async function loadIstatConfigs(supabase) {
  const { data, error } = await supabase
    .from('apartment_istat_config')
    .select('id,apartment_id,attivo,regione,sistema,portal_url,codice_struttura,auth_type,username,deadline_rule,note,export_format,requires_open_close,supports_file_import,supports_webservice')
    .order('created_at', { ascending: false });

  if (error) throw new Error(`load apartment_istat_config failed: ${error.message}`);
  return data || [];
}

async function loadChannelMappings(supabase) {
  const { data, error } = await supabase
    .from('apartment_channel_mappings')
    .select('id,apartment_id,channel,external_name,external_unit_id,active')
    .eq('channel', 'alloggiati');

  if (error) {
    if (/apartment_channel_mappings/i.test(String(error.message || ''))) return [];
    throw new Error(`load apartment_channel_mappings failed: ${error.message}`);
  }
  return data || [];
}

async function loadSyncContext(supabase) {
  const [apartments, links, accounts, istatConfigs, channelMappings] = await Promise.all([
    loadApartments(supabase),
    loadApartmentAlloggiati(supabase),
    loadAlloggiatiAccounts(supabase),
    loadIstatConfigs(supabase),
    loadChannelMappings(supabase),
  ]);

  return {
    apartments,
    apartmentsById: new Map(apartments.map(item => [item.id, item])),
    links,
    linksByApartmentId: new Map(links.map(item => [item.apartment_id, item])),
    accounts,
    accountsById: new Map(accounts.map(item => [item.id, item])),
    istatConfigs,
    istatConfigById: new Map(istatConfigs.map(item => [item.id, item])),
    istatByApartmentId: groupBy(istatConfigs, item => item.apartment_id),
    channelMappings,
    channelMappingsByApartmentId: groupBy(channelMappings, item => item.apartment_id),
  };
}

function buildAuditReport(context, meta = {}) {
  const rows = buildAuditRows(context);
  return {
    generated_at: new Date().toISOString(),
    mode: meta.mode || 'audit',
    schema_checks: {
      public_checkin_key_unique_index_expected: true,
      apartment_alloggiati_unique_apartment_expected: true,
    },
    summary: summarizeRows(rows, meta.stats),
    apartments: rows,
  };
}

function groupBy(list, getKey) {
  const map = new Map();
  (list || []).forEach(item => {
    const key = getKey(item);
    const current = map.get(key) || [];
    current.push(item);
    map.set(key, current);
  });
  return map;
}

function cleanNullable(value) {
  const trimmed = String(value || '').trim();
  return trimmed || null;
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function insertAuditLog(supabase, payload) {
  const { error } = await supabase.from('audit_log').insert({
    user_email: payload.user_email,
    action: payload.action,
    table_name: payload.table_name,
    record_id: payload.record_id,
    timestamp: new Date().toISOString(),
  });
  if (error) {
    console.error('[apartment-link-sync] audit_log error', error);
  }
}

module.exports = {
  REGION_DEFAULTS,
  REGION_SYSTEMS,
  loadSyncContext,
  buildAuditReport,
  applyAuditRows,
  executeApply,
  insertAuditLog,
  normalizeText,
  slugify,
  inferRegionFromApartment,
  buildUniquePublicCheckinKey,
};
