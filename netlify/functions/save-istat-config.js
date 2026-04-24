const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

const REGION_SYSTEMS = {
  'emilia-romagna': 'ross1000',
  'marche': 'istrice_ross1000',
  'veneto': 'ross1000',
  'valle-daosta': 'vit_albergatori',
};

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed' });
  }

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY
  );

  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  const token = authHeader.replace('Bearer ', '');
  const { data: { user }, error: userErr } = await supabase.auth.getUser(token);
  if (userErr || !user) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return jsonResponse(400, { error: 'Invalid JSON' });
  }

  const validationError = validatePayload(body);
  if (validationError) {
    return jsonResponse(400, { error: validationError });
  }

  try {
    const { data: existing, error: existingErr } = await supabase
      .from('apartment_istat_config')
      .select('id,password_encrypted')
      .eq('apartment_id', body.apartment_id)
      .maybeSingle();

    if (existingErr) {
      return jsonResponse(500, { error: 'Errore lettura configurazione esistente', detail: existingErr.message });
    }

    let passwordUpdated = false;
    let passwordEncrypted = existing?.password_encrypted || null;
    const rawPassword = typeof body.password === 'string' ? body.password.trim() : '';

    if (rawPassword) {
      const encryptionValidation = validateEncryptionKey(process.env.ENCRYPTION_KEY);
      if (!encryptionValidation.ok) {
        return jsonResponse(500, { error: 'Configurazione server non valida: ENCRYPTION_KEY mancante o non valida' });
      }
      passwordEncrypted = encrypt(rawPassword);
      passwordUpdated = true;
    }

    const payload = {
      apartment_id: body.apartment_id,
      attivo: body.attivo !== false,
      regione: body.regione,
      sistema: body.sistema,
      portal_url: cleanNullable(body.portal_url),
      codice_struttura: cleanNullable(body.codice_struttura),
      auth_type: cleanNullable(body.auth_type),
      username: cleanNullable(body.username),
      password_encrypted: passwordEncrypted,
      supports_file_import: Boolean(body.supports_file_import),
      supports_webservice: Boolean(body.supports_webservice),
      requires_open_close: Boolean(body.requires_open_close),
      deadline_rule: cleanNullable(body.deadline_rule),
      note: cleanNullable(body.note),
      export_format: cleanNullable(body.export_format) || 'json',
    };

    let result;
    if (existing?.id) {
      const { data, error } = await supabase
        .from('apartment_istat_config')
        .update(payload)
        .eq('id', existing.id)
        .select('id')
        .single();
      if (error) {
        return jsonResponse(500, { error: 'Errore aggiornamento configurazione ISTAT', detail: error.message });
      }
      result = data;
    } else {
      const { data, error } = await supabase
        .from('apartment_istat_config')
        .insert(payload)
        .select('id')
        .single();
      if (error) {
        return jsonResponse(500, { error: 'Errore creazione configurazione ISTAT', detail: error.message });
      }
      result = data;
    }

    return jsonResponse(200, {
      success: true,
      config_id: result.id,
      password_updated: passwordUpdated,
    });
  } catch (err) {
    console.error('[save-istat-config] error:', err);
    return jsonResponse(500, { error: 'Errore interno', detail: err.message });
  }
};

function validatePayload(body) {
  if (!body || typeof body !== 'object') return 'Payload non valido';
  if (!body.apartment_id || typeof body.apartment_id !== 'string') return 'apartment_id richiesto';
  if (!body.regione || typeof body.regione !== 'string') return 'regione richiesta';
  if (!body.sistema || typeof body.sistema !== 'string') return 'sistema richiesto';

  const expectedSystem = REGION_SYSTEMS[body.regione];
  if (!expectedSystem) return `Regione non supportata: ${body.regione}`;
  if (body.sistema !== expectedSystem) {
    return `Configurazione incoerente: ${body.regione} richiede sistema ${expectedSystem}`;
  }

  return null;
}

function cleanNullable(value) {
  if (value == null) return null;
  const str = String(value).trim();
  return str || null;
}

function encrypt(plaintext) {
  const key = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted.toString('hex')}`;
}

function validateEncryptionKey(value) {
  if (!value || typeof value !== 'string') return { ok: false };
  if (!/^[0-9a-fA-F]+$/.test(value) || value.length % 2 !== 0) return { ok: false };
  const key = Buffer.from(value, 'hex');
  if (key.length !== 32) return { ok: false };
  return { ok: true };
}

function jsonResponse(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  };
}
