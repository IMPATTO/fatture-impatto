const sb = window.sb;

const TEAM_TAGS = [
  { value: 'marco', label: 'Marco' },
  { value: 'veronica', label: 'Veronica' },
  { value: 'jessica', label: 'Jessica' },
  { value: 'serena', label: 'Serena' },
  { value: 'victor&vanessa', label: 'Victor&Vanessa' },
];
const LEGACY_EMAIL_PROFILE_MAP = {
  'fatturazione@illupoaffitta.com': 'Marco',
  'contabilita@illupoaffitta.com': 'Veronica',
};

const STATUS_OPTIONS = ['APERTO', 'IN_CORSO', 'COMPLETATO', 'ARCHIVIATO'];
const PRIORITY_OPTIONS = ['BASSA', 'NORMALE', 'ALTA', 'URGENTE'];
const SOURCE_OPTIONS = ['MANUALE', 'NOTA_TELEFONO', 'WHATSAPP', 'SISTEMA'];
const STATUS_LABELS = { APERTO:'Aperto', IN_CORSO:'In corso', COMPLETATO:'Completato', ARCHIVIATO:'Archiviato' };
const PRIORITY_LABELS = { BASSA:'Bassa', NORMALE:'Normale', ALTA:'Alta', URGENTE:'Urgente' };
const SOURCE_LABELS = { MANUALE:'Manuale', NOTA_TELEFONO:'Nota telefono', WHATSAPP:'WhatsApp', SISTEMA:'Sistema' };
const ROLE_LABELS = { admin:'Amministratore', operator:'Operativo', limited:'Area riservata' };
const GENERAL_APARTMENT = '__GENERAL__';

const S = {
  session: null,
  loading: true,
  saving: false,
  tasks: [],
  apartments: [],
  guests: [],
  accessProfile: null,
  accessProfiles: [],
  filters: { search:'', stato:'', priorita:'', apartment_id:'', tag:'', owner_tag:'' },
  selectedId: null,
  drawerOpen: false,
  form: {},
  guestSearch: '',
  quickNote: '',
  quickApartmentId: '',
  quickDueDate: '',
};

const byId = (id) => document.getElementById(id);

function esc(value){ return String(value ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function txt(value){ return String(value ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function fmtDate(value){ if(!value) return '—'; const d = new Date(value); if(Number.isNaN(d.getTime())) return value; return d.toLocaleDateString('it-IT',{day:'2-digit',month:'2-digit',year:'numeric'}); }
function fmtDateTime(value){ if(!value) return '—'; const d = new Date(value); if(Number.isNaN(d.getTime())) return value; return d.toLocaleString('it-IT',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}); }
function normalizeTag(value){ return String(value || '').trim().toLowerCase(); }
function unique(array){ return Array.from(new Set(array)); }
function parseTagsInput(value){ return unique(String(value || '').split(',').map(v => normalizeTag(v)).filter(Boolean)); }
function joinTags(tags){ return Array.isArray(tags) ? tags.join(', ') : ''; }
function toast(msg, type='ok'){ const el=byId('_toast'); el.innerHTML=`<div class="toast toast-${type}">${esc(msg)}</div>`; clearTimeout(S.toastTimer); S.toastTimer=setTimeout(()=>{ el.innerHTML=''; }, 3600); }
function todayIso(){ return new Date().toISOString().slice(0,10); }
function statusClass(value){ return { APERTO:'b-open', IN_CORSO:'b-progress', COMPLETATO:'b-done', ARCHIVIATO:'b-arch' }[value] || 'b-open'; }
function priorityClass(value){ return { BASSA:'p-low', NORMALE:'p-normal', ALTA:'p-high', URGENTE:'p-urgent' }[value] || 'p-normal'; }
function isOpenTask(row){ return row && !['COMPLETATO','ARCHIVIATO'].includes(row.stato); }
function userEmail(){ return String(S.session?.user?.email || '').trim().toLowerCase(); }
function accessRole(){ return String(S.accessProfile?.role || ''); }
function canEdit(){ return ['admin', 'operator'].includes(accessRole()); }
function canManageAccess(){ return accessRole() === 'admin'; }
function ownerTagsOf(row){ return Array.isArray(row?.owner_tags) ? row.owner_tags.map(normalizeTag).filter(Boolean) : []; }
function taskTagsOf(row){ return Array.isArray(row?.tags) ? row.tags.map(normalizeTag).filter(Boolean) : []; }
function myAllowedTags(){ return Array.isArray(S.accessProfile?.allowed_tags) ? S.accessProfile.allowed_tags.map(normalizeTag).filter(Boolean) : []; }
function hasOwnerOverlap(row, tags = myAllowedTags()){
  if (!tags.length) return false;
  const set = new Set(ownerTagsOf(row));
  return tags.some(tag => set.has(normalizeTag(tag)));
}
function overdueTask(row){ return isOpenTask(row) && row?.due_date && row.due_date < todayIso(); }
function effectivePriority(row){ return overdueTask(row) ? 'URGENTE' : (row?.priorita || 'NORMALE'); }
function apartmentNameById(id){
  if (!id || String(id) === GENERAL_APARTMENT) return 'Generale';
  return S.apartments.find(item => String(item.id) === String(id))?.nome_appartamento || 'Generale';
}
function guestById(id){ return S.guests.find(item => String(item.id) === String(id)) || null; }
function guestDisplay(guest){
  if (!guest) return '—';
  const full = [guest.nome, guest.cognome].filter(Boolean).join(' ').trim();
  const tag = guest.tag_prenotazione ? ` · ${guest.tag_prenotazione}` : '';
  return `${full || guest.email || guest.id}${tag}`;
}
function teamLabel(value){ return TEAM_TAGS.find(item => item.value === normalizeTag(value))?.label || value; }
function roleLabel(value){ return ROLE_LABELS[value] || value || '—'; }
function resolveAccessProfile(profiles, email){
  const normalizedEmail = normalizeTag(email);
  if (!normalizedEmail) return null;
  const exact = profiles.find(item => normalizeTag(item.login_email) === normalizedEmail);
  if (exact) return exact;
  const legacyDisplayName = LEGACY_EMAIL_PROFILE_MAP[normalizedEmail];
  if (!legacyDisplayName) return null;
  return profiles.find(item => normalizeTag(item.display_name) === normalizeTag(legacyDisplayName)) || null;
}
function deriveTagsFromText(raw){
  const text = String(raw || '').toLowerCase();
  const tags = [];
  const add = (value) => { if (!tags.includes(value)) tags.push(value); };
  if (/(pulizia|pulire)/.test(text)) add('pulizia');
  if (/(rotto|guasto|sistemare|tecnico)/.test(text)) add('manutenzione');
  if (/(pagamento|soldi|bonifico|contanti)/.test(text)) add('pagamento');
  if (/(documenti|carta|passaporto)/.test(text)) add('documenti');
  if (/(urgente|subito|oggi)/.test(text)) add('urgente');
  return tags;
}
function derivePriorityFromText(raw){ return /(urgente|subito|oggi)/i.test(String(raw || '')) ? 'URGENTE' : 'NORMALE'; }
function deriveTitleFromText(raw){
  const compact = String(raw || '').replace(/\s+/g, ' ').trim();
  if (!compact) return 'Nuova attivita operativa';
  const firstSentence = compact.split(/[.!?\n]/).map(v => v.trim()).find(Boolean) || compact;
  return firstSentence.slice(0, 80);
}
function currentTask(){ return S.tasks.find(item => String(item.id) === String(S.selectedId)) || null; }
function upsertTaskInState(task){
  if (!task?.id) return;
  const next = { ...task };
  const index = S.tasks.findIndex(item => String(item.id) === String(task.id));
  if (index >= 0) {
    S.tasks.splice(index, 1, next);
  } else {
    S.tasks.unshift(next);
  }
}
async function refreshDataAfterMutation(focusTask = null){
  try {
    await loadData();
    if (focusTask?.id) {
      const refreshed = S.tasks.find(item => String(item.id) === String(focusTask.id)) || focusTask;
      openDrawerFor(refreshed);
    } else {
      partialApp();
    }
  } catch (error) {
    console.error('operativita refresh after mutation error', error);
    if (focusTask?.id) {
      upsertTaskInState(focusTask);
      openDrawerFor(focusTask);
    } else {
      partialApp();
    }
    toast('Attività salvata, ma il refresh automatico non è riuscito. I dati sono comunque presenti.', 'info');
  }
}
function emptyTaskForm(){
  return {
    id: '',
    titolo: '',
    descrizione: '',
    stato: 'APERTO',
    priorita: 'NORMALE',
    tags: [],
    owner_tags: [],
    apartment_id: GENERAL_APARTMENT,
    ospiti_check_in_id: '',
    referente_nome: '',
    fonte: 'MANUALE',
    raw_input: '',
    due_date: '',
    created_by: S.session?.user?.email || '',
    completed_at: '',
  };
}
function bestEffortAudit(action, id){
  // Fase 2: webhook WhatsApp Business / provider esterno per creare task da messaggi inbound.
  return sb.from('audit_log').insert({
    user_email: S.session?.user?.email || '',
    action,
    table_name: 'operativita_tasks',
    record_id: String(id || ''),
    timestamp: new Date().toISOString(),
  }).catch(() => null);
}

async function loadData(){
  S.loading = true;
  partialApp();
  const { data: profiles, error: profilesError } = await sb.from('operativita_access_profiles').select('*').order('sort_order');
  if (profilesError) throw profilesError;
  S.accessProfiles = profiles || [];
  S.accessProfile = resolveAccessProfile(S.accessProfiles, userEmail());
  if (!S.accessProfile) {
    S.tasks = [];
    S.apartments = [];
    S.guests = [];
    S.loading = false;
    partialApp();
    return;
  }
  const [tasksRes, apartmentsRes] = await Promise.all([
    sb.from('operativita_tasks').select('*').order('due_date', { ascending: true, nullsFirst: false }).order('created_at', { ascending: false }).limit(500),
    sb.from('apartments').select('id,nome_appartamento').order('nome_appartamento'),
  ]);
  if (tasksRes.error) throw tasksRes.error;
  S.tasks = tasksRes.data || [];
  S.apartments = apartmentsRes.error ? [] : (apartmentsRes.data || []);
  if (!S.apartments.length && S.session?.access_token) {
    S.apartments = await loadOperativitaApartmentsFallback();
  }

  if (canEdit()) {
    const guestsRes = await sb.from('ospiti_check_in')
      .select('id,nome,cognome,email,tag_prenotazione,data_checkin,data_checkout,apartment_id')
      .order('data_checkin', { ascending: false })
      .limit(500);
    if (guestsRes.error) throw guestsRes.error;
    S.guests = guestsRes.data || [];
  } else {
    const guestIds = unique(S.tasks.map(row => row.ospiti_check_in_id).filter(Boolean));
    if (!guestIds.length) {
      S.guests = [];
    } else {
      const guestsRes = await sb.from('ospiti_check_in')
        .select('id,nome,cognome,email,tag_prenotazione,data_checkin,data_checkout,apartment_id')
        .in('id', guestIds);
      if (guestsRes.error) throw guestsRes.error;
      S.guests = guestsRes.data || [];
    }
  }

  if (S.selectedId && !currentTask()) {
    S.selectedId = null;
    S.drawerOpen = false;
    S.form = {};
  }
  S.loading = false;
  partialApp();
}

async function loadOperativitaApartmentsFallback() {
  try {
    const res = await fetch('/.netlify/functions/get-operativita-apartments', {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${S.session.access_token}`,
      },
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(payload.error || payload.detail || 'Errore caricamento appartamenti');
    }
    return Array.isArray(payload.apartments) ? payload.apartments : [];
  } catch (error) {
    toast(`Lista appartamenti non disponibile: ${error.message}`, 'err');
    return [];
  }
}

function stats(){
  const rows = filteredTasks();
  const open = rows.filter(r => isOpenTask(r)).length;
  const urgent = rows.filter(r => effectivePriority(r) === 'URGENTE' && isOpenTask(r)).length;
  const today = rows.filter(r => r.due_date === todayIso() && isOpenTask(r)).length;
  const completed = rows.filter(r => r.stato === 'COMPLETATO').length;
  return { open, urgent, today, completed };
}

function uniqueTags(){
  return unique(S.tasks.flatMap(row => taskTagsOf(row))).sort();
}

function filteredTasks(){
  const q = String(S.filters.search || '').trim().toLowerCase();
  const tag = normalizeTag(S.filters.tag);
  const ownerTag = normalizeTag(S.filters.owner_tag);
  return S.tasks.filter(row => {
    if (S.filters.stato && row.stato !== S.filters.stato) return false;
    if (S.filters.priorita && effectivePriority(row) !== S.filters.priorita) return false;
    if (S.filters.apartment_id === GENERAL_APARTMENT && row.apartment_id) return false;
    if (S.filters.apartment_id && S.filters.apartment_id !== GENERAL_APARTMENT && String(row.apartment_id || '') !== String(S.filters.apartment_id)) return false;
    if (tag && !taskTagsOf(row).includes(tag)) return false;
    if (ownerTag && !ownerTagsOf(row).includes(ownerTag)) return false;
    if (!q) return true;
    const guest = guestById(row.ospiti_check_in_id);
    const haystack = [
      row.titolo, row.descrizione, row.referente_nome, row.fonte, row.raw_input,
      apartmentNameById(row.apartment_id), guest?.nome, guest?.cognome, guest?.email, guest?.tag_prenotazione,
      ...taskTagsOf(row), ...ownerTagsOf(row),
    ].join(' ').toLowerCase();
    return haystack.includes(q);
  });
}

function selectedOwnerLabel(){
  const selected = normalizeTag(S.filters.owner_tag);
  if (selected) return teamLabel(selected);
  if (accessRole() === 'limited') {
    const labels = (S.accessProfile?.allowed_tags || []).map(teamLabel).filter(Boolean);
    return labels.join(', ') || 'me';
  }
  return 'tutti';
}

function tasksAssignedToday(){
  return filteredTasks().filter(row => isOpenTask(row) && row.due_date === todayIso())
    .sort((a, b) => (effectivePriority(b) === 'URGENTE') - (effectivePriority(a) === 'URGENTE') || String(a.titolo || '').localeCompare(String(b.titolo || '')));
}

function importantUpcomingTasks(){
  const priorityWeight = { URGENTE: 4, ALTA: 3, NORMALE: 2, BASSA: 1 };
  return filteredTasks()
    .filter(row => isOpenTask(row))
    .sort((a, b) => {
      const overdueDelta = Number(overdueTask(b)) - Number(overdueTask(a));
      if (overdueDelta) return overdueDelta;
      const prioDelta = (priorityWeight[effectivePriority(b)] || 0) - (priorityWeight[effectivePriority(a)] || 0);
      if (prioDelta) return prioDelta;
      const dateA = a.due_date || '9999-12-31';
      const dateB = b.due_date || '9999-12-31';
      return String(dateA).localeCompare(String(dateB)) || String(a.titolo || '').localeCompare(String(b.titolo || ''));
    })
    .slice(0, 6);
}

function visibleGuestMatches(){
  if (!canEdit()) return [];
  const q = String(S.guestSearch || '').trim().toLowerCase();
  if (!q) return [];
  return S.guests.filter(guest => {
    const haystack = [guest.nome, guest.cognome, guest.email, guest.tag_prenotazione, apartmentNameById(guest.apartment_id)].join(' ').toLowerCase();
    return haystack.includes(q);
  }).slice(0, 12);
}

function openDrawerFor(task){
  S.selectedId = task?.id || null;
  S.drawerOpen = true;
  S.guestSearch = '';
  S.form = task ? {
    id: task.id,
    titolo: task.titolo || '',
    descrizione: task.descrizione || '',
    stato: task.stato || 'APERTO',
    priorita: task.priorita || 'NORMALE',
    tags: Array.isArray(task.tags) ? [...task.tags] : [],
    owner_tags: Array.isArray(task.owner_tags) ? [...task.owner_tags] : [],
    apartment_id: task.apartment_id || GENERAL_APARTMENT,
    ospiti_check_in_id: task.ospiti_check_in_id || '',
    referente_nome: task.referente_nome || '',
    fonte: task.fonte || 'MANUALE',
    raw_input: task.raw_input || '',
    due_date: task.due_date || '',
    created_by: task.created_by || '',
    completed_at: task.completed_at || '',
  } : emptyTaskForm();
  partialApp();
}

function closeDrawer(){
  S.drawerOpen = false;
  S.selectedId = null;
  S.guestSearch = '';
  S.form = {};
  partialApp();
}

function createManualTask(){
  if (!canEdit()) return;
  openDrawerFor(null);
}

function toggleOwnerTag(tag){
  if (!canEdit()) return;
  const value = normalizeTag(tag);
  const set = new Set((S.form.owner_tags || []).map(normalizeTag));
  if (set.has(value)) set.delete(value); else set.add(value);
  S.form.owner_tags = Array.from(set);
  partialApp();
}

async function createFromQuickNote(){
  if (!canEdit()) return;
  const raw = String(S.quickNote || '').trim();
  if (!raw) {
    toast('Incolla prima una nota operativa', 'err');
    return;
  }
  const payload = {
    titolo: deriveTitleFromText(raw),
    descrizione: raw,
    stato: 'APERTO',
    priorita: derivePriorityFromText(raw),
    tags: deriveTagsFromText(raw),
    owner_tags: [...myAllowedTags()],
    apartment_id: !S.quickApartmentId || S.quickApartmentId === GENERAL_APARTMENT ? null : S.quickApartmentId,
    ospiti_check_in_id: null,
    referente_nome: '',
    fonte: 'NOTA_TELEFONO',
    raw_input: raw,
    due_date: S.quickDueDate || null,
    created_by: S.session?.user?.email || '',
  };
  S.saving = true;
  partialApp();
  const { data, error } = await sb.from('operativita_tasks').insert(payload).select('*').single();
  S.saving = false;
  if (error) {
    toast('Errore creazione nota: ' + error.message, 'err');
    partialApp();
    return;
  }
  await bestEffortAudit('create_note', data.id);
  S.quickNote = '';
  S.quickDueDate = '';
  upsertTaskInState(data);
  partialApp();
  toast('✓ Nota operativa salvata', 'ok');
  await refreshDataAfterMutation(data);
}

async function saveTask(){
  if (!canEdit()) return;
  const payload = {
    titolo: String(S.form.titolo || '').trim(),
    descrizione: String(S.form.descrizione || '').trim() || null,
    stato: S.form.stato || 'APERTO',
    priorita: S.form.priorita || 'NORMALE',
    tags: Array.isArray(S.form.tags) ? S.form.tags : [],
    owner_tags: Array.isArray(S.form.owner_tags) ? S.form.owner_tags.map(normalizeTag).filter(Boolean) : [],
    apartment_id: !S.form.apartment_id || S.form.apartment_id === GENERAL_APARTMENT ? null : S.form.apartment_id,
    ospiti_check_in_id: S.form.ospiti_check_in_id || null,
    referente_nome: String(S.form.referente_nome || '').trim() || null,
    fonte: S.form.fonte || 'MANUALE',
    raw_input: String(S.form.raw_input || '').trim() || null,
    due_date: S.form.due_date || null,
    created_by: S.form.created_by || S.session?.user?.email || null,
    completed_at: S.form.stato === 'COMPLETATO' ? (S.form.completed_at || new Date().toISOString()) : null,
  };
  if (!payload.titolo) {
    toast('Il titolo è obbligatorio', 'err');
    return;
  }
  S.saving = true;
  partialApp();
  const response = S.form.id
    ? await sb.from('operativita_tasks').update(payload).eq('id', S.form.id).select('*').single()
    : await sb.from('operativita_tasks').insert(payload).select('*').single();
  S.saving = false;
  if (response.error) {
    toast('Errore salvataggio: ' + response.error.message, 'err');
    partialApp();
    return;
  }
  await bestEffortAudit(S.form.id ? 'update' : 'create', response.data.id);
  upsertTaskInState(response.data);
  openDrawerFor(response.data);
  toast(S.form.id ? '✓ Attività aggiornata' : '✓ Attività creata', 'ok');
  await refreshDataAfterMutation(response.data);
}

async function updateSelectedTask(patch, successMessage, auditAction){
  if (!canEdit()) return;
  const task = currentTask();
  if (!task) return;
  S.saving = true;
  partialApp();
  const payload = {
    ...patch,
    completed_at: patch.stato === 'COMPLETATO'
      ? (patch.completed_at || task.completed_at || new Date().toISOString())
      : (patch.stato && patch.stato !== 'COMPLETATO' ? null : task.completed_at),
  };
  const { data, error } = await sb.from('operativita_tasks').update(payload).eq('id', task.id).select('*').single();
  S.saving = false;
  if (error) {
    toast('Errore aggiornamento: ' + error.message, 'err');
    partialApp();
    return;
  }
  await bestEffortAudit(auditAction, task.id);
  upsertTaskInState(data);
  openDrawerFor(data);
  toast(successMessage, 'ok');
  await refreshDataAfterMutation(data);
}

async function saveAccessProfile(id){
  if (!canManageAccess()) return;
  const loginEmail = String(document.querySelector(`[data-access-email="${id}"]`)?.value || '').trim().toLowerCase() || null;
  const role = String(document.querySelector(`[data-access-role="${id}"]`)?.value || 'operator');
  const allowedTags = parseTagsInput(document.querySelector(`[data-access-tags="${id}"]`)?.value || '');
  const { error } = await sb.from('operativita_access_profiles')
    .update({ login_email: loginEmail, role, allowed_tags: allowedTags })
    .eq('id', id);
  if (error) {
    toast('Errore salvataggio accesso: ' + error.message, 'err');
    return;
  }
  toast('✓ Accesso aggiornato', 'ok');
  await loadData();
}

function renderTaskList(items, emptyMessage){
  if (!items.length) return `<div class="helper-copy">${esc(emptyMessage)}</div>`;
  return `<div class="todo-list">${items.map(row => {
    const guest = guestById(row.ospiti_check_in_id);
    const effective = effectivePriority(row);
    return `
      <div class="todo-item" data-task-id="${row.id}">
        <div class="todo-main">${esc(row.titolo || 'Senza titolo')}</div>
        <div class="todo-sub">${esc(apartmentNameById(row.apartment_id))} · ${guest ? esc(guestDisplay(guest)) : 'senza ospite collegato'}</div>
        <div class="todo-meta">
          <span class="badge ${priorityClass(effective)}">${esc(PRIORITY_LABELS[effective] || effective)}</span>
          <span class="badge ${statusClass(row.stato)}">${esc(STATUS_LABELS[row.stato] || row.stato)}</span>
          <span class="badge b-open">${overdueTask(row) ? 'Scaduta' : `Scade ${fmtDate(row.due_date)}`}</span>
        </div>
      </div>`;
  }).join('')}</div>`;
}

function renderOwnerChips(readOnly = false){
  return `<div class="owner-picks">${TEAM_TAGS.map(item => {
    const active = (S.form.owner_tags || []).map(normalizeTag).includes(item.value);
    return `<button type="button" class="owner-chip ${active ? 'active' : ''}" data-owner-tag="${item.value}" ${readOnly ? 'disabled' : ''}>${esc(item.label)}</button>`;
  }).join('')}</div>`;
}

function renderAccessManager(){
  return '';
}

function bindDashboardEvents(){
  byId('btnLogout')?.addEventListener('click', ()=>sb.auth.signOut());
  byId('navbarToggle')?.addEventListener('click', ()=>{
    const menu = byId('navbarMenu');
    const expanded = menu.classList.toggle('open');
    byId('navbarToggle').setAttribute('aria-expanded', expanded ? 'true' : 'false');
  });
  byId('fSearch')?.addEventListener('input', e=>{ S.filters.search = e.target.value; partialApp(); });
  byId('fStato')?.addEventListener('change', e=>{ S.filters.stato = e.target.value; partialApp(); });
  byId('fPriorita')?.addEventListener('change', e=>{ S.filters.priorita = e.target.value; partialApp(); });
  byId('fApartment')?.addEventListener('change', e=>{ S.filters.apartment_id = e.target.value; partialApp(); });
  byId('fTag')?.addEventListener('change', e=>{ S.filters.tag = e.target.value; partialApp(); });
  byId('fOwnerTag')?.addEventListener('change', e=>{ S.filters.owner_tag = e.target.value; partialApp(); });
  byId('btnNewTask')?.addEventListener('click', createManualTask);
  byId('quickNote')?.addEventListener('input', e=>{ S.quickNote = e.target.value; });
  byId('quickApartmentId')?.addEventListener('change', e=>{ S.quickApartmentId = e.target.value; });
  byId('quickDueDate')?.addEventListener('change', e=>{ S.quickDueDate = e.target.value; });
  byId('btnQuickNote')?.addEventListener('click', createFromQuickNote);
  document.querySelectorAll('[data-task-id]').forEach(el => el.addEventListener('click', ()=> {
    const task = S.tasks.find(item => String(item.id) === String(el.dataset.taskId));
    if (task) openDrawerFor(task);
  }));
  document.querySelectorAll('[data-save-access]').forEach(el => el.addEventListener('click', ()=>saveAccessProfile(el.dataset.saveAccess)));
  byId('drawerClose')?.addEventListener('click', closeDrawer);
  byId('taskTitolo')?.addEventListener('input', e=>{ S.form.titolo = e.target.value; });
  byId('taskDescrizione')?.addEventListener('input', e=>{ S.form.descrizione = e.target.value; });
  byId('taskStato')?.addEventListener('change', e=>{ S.form.stato = e.target.value; });
  byId('taskPriorita')?.addEventListener('change', e=>{ S.form.priorita = e.target.value; });
  byId('taskApartment')?.addEventListener('change', e=>{ S.form.apartment_id = e.target.value; });
  byId('taskReferente')?.addEventListener('input', e=>{ S.form.referente_nome = e.target.value; });
  byId('taskFonte')?.addEventListener('change', e=>{ S.form.fonte = e.target.value; });
  byId('taskRawInput')?.addEventListener('input', e=>{ S.form.raw_input = e.target.value; });
  byId('taskDueDate')?.addEventListener('change', e=>{ S.form.due_date = e.target.value; });
  byId('taskTags')?.addEventListener('input', e=>{ S.form.tags = parseTagsInput(e.target.value); });
  byId('guestSearch')?.addEventListener('input', e=>{ S.guestSearch = e.target.value; partialApp(); });
  document.querySelectorAll('[data-guest-id]').forEach(el => el.addEventListener('click', ()=> {
    const guest = guestById(el.dataset.guestId);
    if (!guest) return;
    S.form.ospiti_check_in_id = guest.id;
    if (!S.form.apartment_id && guest.apartment_id) S.form.apartment_id = guest.apartment_id;
    S.guestSearch = '';
    partialApp();
  }));
  byId('clearGuestLink')?.addEventListener('click', ()=>{
    S.form.ospiti_check_in_id = '';
    partialApp();
  });
  document.querySelectorAll('[data-owner-tag]').forEach(el => el.addEventListener('click', ()=>toggleOwnerTag(el.dataset.ownerTag)));
  byId('btnSaveTask')?.addEventListener('click', saveTask);
  byId('btnCompleteTask')?.addEventListener('click', ()=>updateSelectedTask({ stato:'COMPLETATO' }, '✓ Attività completata', 'complete'));
  byId('btnArchiveTask')?.addEventListener('click', ()=>updateSelectedTask({ stato:'ARCHIVIATO' }, '✓ Attività archiviata', 'archive'));
  byId('btnReopenTask')?.addEventListener('click', ()=>updateSelectedTask({ stato:'APERTO' }, '✓ Attività riaperta', 'reopen'));
}

function renderLogin(){
  byId('app').innerHTML = `
    <div class="login-wrap">
      <div class="login-card">
        <div class="login-logo">Impatto</div>
        <div class="login-sub">Backoffice · Operativita</div>
        <div id="lerr" class="msg-err"></div>
        <div class="field"><label>Email</label><input id="lem" type="email" autocomplete="username"/></div>
        <div class="field"><label>Password</label><input id="lpw" type="password" autocomplete="current-password"/></div>
        <button id="lbtn" class="btn btn-primary btn-full">Entra →</button>
      </div>
    </div>`;
  const doLogin = async () => {
    const btn = byId('lbtn');
    const err = byId('lerr');
    err.style.display = 'none';
    btn.disabled = true;
    btn.textContent = 'Accesso…';
    const { error } = await sb.auth.signInWithPassword({
      email: byId('lem').value,
      password: byId('lpw').value,
    });
    if (error) {
      err.textContent = error.message;
      err.style.display = 'block';
      btn.disabled = false;
      btn.textContent = 'Entra →';
    }
  };
  byId('lbtn').addEventListener('click', doLogin);
  byId('lpw').addEventListener('keydown', e=>{ if(e.key==='Enter') doLogin(); });
  setTimeout(()=>byId('lpw')?.focus(), 80);
}

function renderBlocked(){
  byId('app').innerHTML = `
    <header class="section-header">
      <div class="section-header-left">
        <span class="section-header-brand">Impatto</span>
        <span class="section-header-tag">OPERATIVITA</span>
      </div>
      <div class="section-header-user">${esc(S.session?.user?.email || '')}</div>
    </header>
    <section class="blocked-card">
      <div class="blocked-title">Accesso non configurato</div>
      <div class="blocked-copy">
        Questo account non è ancora collegato a un profilo operativo.
        <br/><br/>
        Un amministratore deve assegnarti una riga in <strong>Accessi operatività</strong> indicando la tua email e il tuo ruolo.
        <br/><br/>
        Profili previsti: Marco, Veronica, Jessica, Serena, Victor&Vanessa.
      </div>
      <div style="margin-top:16px"><button id="btnLogout" class="btn btn-ghost">Esci</button></div>
    </section>`;
  byId('navUser').textContent = '';
  byId('navUser').style.display = 'none';
  byId('btnLogout')?.addEventListener('click', ()=>sb.auth.signOut());
}

function renderLoadingScreen(){
  byId('app').innerHTML = `
    <header class="section-header">
      <div class="section-header-left">
        <span class="section-header-brand">Impatto</span>
        <span class="section-header-tag">OPERATIVITA</span>
      </div>
      <div class="section-header-user">${esc(S.session?.user?.email || '')}</div>
    </header>
    <div class="loading"><span class="spin"></span>Carico operativita…</div>`;
}

function renderDashboard(){
  if (S.loading) {
    renderLoadingScreen();
    return;
  }
  if (!S.accessProfile) {
    renderBlocked();
    return;
  }
  const st = stats();
  const rows = filteredTasks();
  const aptOptions = [`<option value="${GENERAL_APARTMENT}" ${String(GENERAL_APARTMENT)===String(S.filters.apartment_id) ? 'selected' : ''}>Generale</option>`]
    .concat(S.apartments.map(item => `<option value="${item.id}" ${String(item.id)===String(S.filters.apartment_id) ? 'selected' : ''}>${txt(item.nome_appartamento)}</option>`))
    .join('');
  const drawerTask = currentTask();
  const linkedGuest = guestById(S.form.ospiti_check_in_id);
  const guestResults = visibleGuestMatches();
  const tagOptions = uniqueTags().map(tag => `<option value="${esc(tag)}" ${tag===normalizeTag(S.filters.tag) ? 'selected' : ''}>${txt(tag)}</option>`).join('');
  const todayAssigned = tasksAssignedToday();
  const nextImportant = importantUpcomingTasks();
  const ownerScopeLabel = selectedOwnerLabel();
  const readOnly = !canEdit();
  byId('app').innerHTML = `
    <header class="section-header">
      <div class="section-header-left">
        <span class="section-header-brand">Impatto</span>
        <span class="section-header-tag">OPERATIVITA</span>
      </div>
      <div class="section-header-user">${esc(S.session?.user?.email || '')} · <span class="role-badge">${esc(roleLabel(accessRole()))}</span></div>
    </header>
    <section class="stats-row">
      <div class="stat-card"><div class="stat-label">Aperte</div><div class="stat-val">${st.open}</div></div>
      <div class="stat-card"><div class="stat-label">Urgenti</div><div class="stat-val">${st.urgent}</div></div>
      <div class="stat-card"><div class="stat-label">Scadenza oggi</div><div class="stat-val">${st.today}</div></div>
      <div class="stat-card"><div class="stat-label">Completate</div><div class="stat-val">${st.completed}</div></div>
    </section>
    <section class="filters">
      <input id="fSearch" placeholder="Cerca titolo, note, tag, ospite, email..." value="${esc(S.filters.search)}"/>
      <select id="fStato">
        <option value="">Tutti gli stati</option>
        ${STATUS_OPTIONS.map(v => `<option value="${v}" ${v===S.filters.stato?'selected':''}>${STATUS_LABELS[v]}</option>`).join('')}
      </select>
      <select id="fPriorita">
        <option value="">Tutte le priorita</option>
        ${PRIORITY_OPTIONS.map(v => `<option value="${v}" ${v===S.filters.priorita?'selected':''}>${PRIORITY_LABELS[v]}</option>`).join('')}
      </select>
      <select id="fApartment">
        <option value="">Tutti gli appartamenti</option>
        ${aptOptions}
      </select>
      <select id="fTag">
        <option value="">Tutti i tag operativi</option>
        ${tagOptions}
      </select>
      <select id="fOwnerTag">
        <option value="">Tutti gli assegnatari</option>
        ${TEAM_TAGS.map(item => `<option value="${item.value}" ${item.value===normalizeTag(S.filters.owner_tag)?'selected':''}>${esc(item.label)}</option>`).join('')}
      </select>
      ${canEdit() ? `<button id="btnNewTask" class="btn btn-primary">Nuova attivita</button>` : ''}
      <button id="btnLogout" class="btn btn-ghost">Esci</button>
    </section>
    <section class="focus-row">
      <div class="card">
        <div class="card-head">
          <div>
            <div class="card-title">${normalizeTag(S.filters.owner_tag) ? `Assegnati a ${esc(ownerScopeLabel)} oggi` : 'Attività di oggi'}</div>
            <div class="card-copy">${normalizeTag(S.filters.owner_tag) ? `Vista filtrata solo sui task assegnati a ${esc(ownerScopeLabel)}.` : 'Questa fascia si aggiorna subito quando cambi assegnatario o altri filtri.'}</div>
          </div>
        </div>
        ${renderTaskList(todayAssigned, normalizeTag(S.filters.owner_tag) ? `Nessun compito assegnato oggi a ${ownerScopeLabel}.` : 'Nessun compito assegnato per oggi.')}
      </div>
      <div class="card">
        <div class="card-head">
          <div>
            <div class="card-title">${normalizeTag(S.filters.owner_tag) ? `Prossimi task di ${esc(ownerScopeLabel)}` : 'Prossimi task importanti'}</div>
            <div class="card-copy">${normalizeTag(S.filters.owner_tag) ? `Vedi solo i prossimi task assegnati a ${esc(ownerScopeLabel)}.` : 'Task aperti ordinati per urgenza, scadenza e priorità.'}</div>
          </div>
        </div>
        ${renderTaskList(nextImportant, normalizeTag(S.filters.owner_tag) ? `Nessun task aperto assegnato a ${ownerScopeLabel}.` : 'Nessun task importante in vista.')}
      </div>
    </section>
    <section class="notes-bar">
      <div class="card">
        <div class="card-head">
          <div>
            <div class="card-title">${canEdit() ? 'Incolla nota operativa' : 'Accesso area assegnata'}</div>
            <div class="card-copy">${canEdit()
              ? 'Incolla un testo dal telefono e lo trasformiamo in task con titolo, priorità e tag suggeriti localmente.'
              : 'Questo profilo vede solo le attività assegnate al proprio tag. La modifica è bloccata a livello database oltre che nella UI.'}</div>
          </div>
        </div>
        ${canEdit() ? `
          <div class="quick-row">
            <div class="field" style="margin-bottom:0">
              <label>Nota</label>
              <textarea id="quickNote" placeholder="Es. oggi sistemare doccia apt 12, ospite ha pagato contanti, chiamare tecnico...">${txt(S.quickNote)}</textarea>
            </div>
            <div class="field" style="margin-bottom:0">
              <label>Appartamento</label>
              <select id="quickApartmentId">
                <option value="${GENERAL_APARTMENT}" ${String(GENERAL_APARTMENT)===String(S.quickApartmentId)?'selected':''}>Generale</option>
                ${S.apartments.map(item => `<option value="${item.id}" ${String(item.id)===String(S.quickApartmentId)?'selected':''}>${txt(item.nome_appartamento)}</option>`).join('')}
              </select>
            </div>
            <div class="field" style="margin-bottom:0">
              <label>Scadenza</label>
              <input id="quickDueDate" type="date" value="${esc(S.quickDueDate)}"/>
            </div>
            <button id="btnQuickNote" class="btn btn-green" ${S.saving ? 'disabled' : ''}>Crea da nota</button>
          </div>
        ` : `<div class="hint-box">Area in sola consultazione per il tag: <strong>${esc((S.accessProfile.allowed_tags || []).map(teamLabel).join(', ') || '—')}</strong></div>`}
      </div>
    </section>
    <section class="tips-row">
      <div class="hint-box">
        Suggerimento operativo:
        <br/>1. assegna sempre una persona oltre ai tag di categoria
        <br/>2. se selezioni un assegnatario vedi solo i suoi task
        <br/>3. metti una scadenza su tutto ciò che non è “someday”
        <br/>4. ogni task scaduta resta aperta ma viene trattata come urgente
      </div>
    </section>
    ${renderAccessManager()}
    <section class="body-wrap">
      <div class="table-panel">
        ${S.loading ? `<div class="loading"><span class="spin"></span>Carico operativita…</div>` : rows.length ? `
          <table>
            <thead>
              <tr>
                <th>Attivita</th>
                <th>Assegnato</th>
                <th>Stato</th>
                <th>Priorita</th>
                <th>Collegamenti</th>
                <th>Scadenza</th>
                <th>Creata</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map(row => {
                const guest = guestById(row.ospiti_check_in_id);
                const effective = effectivePriority(row);
                return `
                  <tr data-task-id="${row.id}" class="${String(row.id)===String(S.selectedId) ? 'selected' : ''}">
                    <td>
                      <div class="td-title">${esc(row.titolo || 'Senza titolo')}</div>
                      <div class="td-meta">${esc((row.descrizione || '').slice(0, 140))}${(row.descrizione || '').length > 140 ? '…' : ''}</div>
                      ${taskTagsOf(row).length ? `<div class="tag-list">${taskTagsOf(row).map(tag => `<span class="tag-chip">${esc(tag)}</span>`).join('')}</div>` : ''}
                    </td>
                    <td>${ownerTagsOf(row).length ? `<div class="tag-list">${ownerTagsOf(row).map(tag => `<span class="tag-chip">${esc(teamLabel(tag))}</span>`).join('')}</div>` : `<div class="td-meta">Non assegnato</div>`}</td>
                    <td><span class="badge ${statusClass(row.stato)}">${esc(STATUS_LABELS[row.stato] || row.stato)}</span></td>
                    <td><span class="badge ${priorityClass(effective)}">${esc(PRIORITY_LABELS[effective] || effective)}</span></td>
                    <td>
                      <div>${esc(apartmentNameById(row.apartment_id))}</div>
                      ${guest ? `<div class="td-meta">${esc(guestDisplay(guest))}</div>` : `<div class="td-meta">Nessun ospite collegato</div>`}
                    </td>
                    <td class="td-date">${row.due_date ? fmtDate(row.due_date) : '—'}${overdueTask(row) ? '<br/><span style="color:#b91c1c">Scaduta</span>' : ''}</td>
                    <td class="td-date">${fmtDateTime(row.created_at)}</td>
                  </tr>`;
              }).join('')}
            </tbody>
          </table>
        ` : `<div class="empty">Nessuna attività trovata con questi filtri.</div>`}
      </div>
      <aside class="drawer ${S.drawerOpen ? 'open' : ''}">
        ${S.drawerOpen ? `
          <div class="drawer-header">
            <div>
              <div class="drawer-title">${esc(S.form.id ? 'Dettaglio attivita' : 'Nuova attivita')}</div>
              <div class="drawer-sub">${esc(S.form.id ? `ID ${S.form.id}` : 'Compila i campi e salva')}</div>
            </div>
            <button id="drawerClose" class="drawer-close" aria-label="Chiudi">✕</button>
          </div>
          <div class="drawer-body">
            <div class="drawer-sec">Dati principali</div>
            <div class="field"><label>Titolo</label><input id="taskTitolo" value="${esc(S.form.titolo || '')}" placeholder="Es. Chiamare tecnico per climatizzatore" ${readOnly ? 'disabled' : ''}/></div>
            <div class="field"><label>Descrizione</label><textarea id="taskDescrizione" placeholder="Dettagli operativi, cose da fare, note interne..." ${readOnly ? 'disabled' : ''}>${txt(S.form.descrizione || '')}</textarea></div>
            <div class="field-2">
              <div class="field"><label>Stato</label><select id="taskStato" ${readOnly ? 'disabled' : ''}>${STATUS_OPTIONS.map(v => `<option value="${v}" ${v===S.form.stato?'selected':''}>${STATUS_LABELS[v]}</option>`).join('')}</select></div>
              <div class="field"><label>Priorita</label><select id="taskPriorita" ${readOnly ? 'disabled' : ''}>${PRIORITY_OPTIONS.map(v => `<option value="${v}" ${v===S.form.priorita?'selected':''}>${PRIORITY_LABELS[v]}</option>`).join('')}</select></div>
            </div>
            <div class="field">
              <label>Assegnato a</label>
              ${renderOwnerChips(readOnly)}
            </div>
            <div class="field"><label>Tag categoria</label><input id="taskTags" value="${esc(joinTags(S.form.tags))}" placeholder="pulizia, manutenzione, pagamento" ${readOnly ? 'disabled' : ''}/></div>
            <div class="field-2">
              <div class="field">
                <label>Appartamento</label>
                <select id="taskApartment" ${readOnly ? 'disabled' : ''}>
                  <option value="${GENERAL_APARTMENT}" ${String(GENERAL_APARTMENT)===String(S.form.apartment_id)?'selected':''}>Generale</option>
                  ${S.apartments.map(item => `<option value="${item.id}" ${String(item.id)===String(S.form.apartment_id)?'selected':''}>${txt(item.nome_appartamento)}</option>`).join('')}
                </select>
              </div>
              <div class="field"><label>Scadenza</label><input id="taskDueDate" type="date" value="${esc(S.form.due_date || '')}" ${readOnly ? 'disabled' : ''}/></div>
            </div>
            <div class="field-2">
              <div class="field"><label>Referente</label><input id="taskReferente" value="${esc(S.form.referente_nome || '')}" placeholder="Es. Sara / housekeeping / manutentore" ${readOnly ? 'disabled' : ''}/></div>
              <div class="field"><label>Fonte</label><select id="taskFonte" ${readOnly ? 'disabled' : ''}>${SOURCE_OPTIONS.map(v => `<option value="${v}" ${v===S.form.fonte?'selected':''}>${SOURCE_LABELS[v]}</option>`).join('')}</select></div>
            </div>

            <div class="drawer-sec">Collegamento ospite / check-in</div>
            ${canEdit() ? `
              <div class="field"><label>Cerca ospite o prenotazione</label><input id="guestSearch" value="${esc(S.guestSearch)}" placeholder="Nome, cognome, email o tag prenotazione"/></div>
              ${guestResults.length ? `<div class="search-results">${guestResults.map(guest => `
                <button type="button" class="result-btn" data-guest-id="${guest.id}">
                  <div class="result-main">${esc(guestDisplay(guest))}</div>
                  <div class="result-sub">${esc(apartmentNameById(guest.apartment_id))} · ${fmtDate(guest.data_checkin)} → ${fmtDate(guest.data_checkout)}</div>
                </button>
              `).join('')}</div>` : (S.guestSearch ? `<div class="hint-box">Nessun ospite trovato con questa ricerca.</div>` : '')}
            ` : ''}
            <div class="linked-box" style="margin-top:10px">
              <strong>Check-in collegato:</strong> ${linkedGuest ? esc(guestDisplay(linkedGuest)) : 'Nessuno'}
              ${linkedGuest ? `<br/><span style="color:var(--text2)">${esc(apartmentNameById(linkedGuest.apartment_id))} · ${fmtDate(linkedGuest.data_checkin)} → ${fmtDate(linkedGuest.data_checkout)}</span>${canEdit() ? `<br/><button id="clearGuestLink" class="btn btn-ghost" style="margin-top:10px;padding:7px 12px;font-size:12px">Rimuovi collegamento</button>` : ''}` : ''}
            </div>

            <div class="drawer-sec">Nota originale</div>
            <div class="field"><label>Raw input</label><textarea id="taskRawInput" placeholder="Testo originale della nota telefono o del messaggio" ${readOnly ? 'disabled' : ''}>${txt(S.form.raw_input || '')}</textarea></div>
            <div class="hint-box">
              Creato da: <strong>${esc(S.form.created_by || S.session?.user?.email || '—')}</strong>
              <br/>Creato il: <strong>${drawerTask ? fmtDateTime(drawerTask.created_at) : '—'}</strong>
              <br/>Aggiornato il: <strong>${drawerTask ? fmtDateTime(drawerTask.updated_at) : '—'}</strong>
              ${drawerTask?.completed_at ? `<br/>Completato il: <strong>${fmtDateTime(drawerTask.completed_at)}</strong>` : ''}
            </div>
          </div>
          ${canEdit() ? `
            <div class="drawer-footer">
              <button id="btnSaveTask" class="btn btn-primary" ${S.saving ? 'disabled' : ''}>Salva</button>
              ${S.form.id ? `
                <button id="btnCompleteTask" class="btn btn-green" ${S.saving ? 'disabled' : ''}>Completa</button>
                <button id="btnArchiveTask" class="btn btn-yellow" ${S.saving ? 'disabled' : ''}>Archivia</button>
                <button id="btnReopenTask" class="btn btn-ghost" ${S.saving ? 'disabled' : ''}>Riapri</button>
              ` : ''}
            </div>
          ` : ''}
        ` : ''}
      </aside>
    </section>`;
  byId('navUser').textContent = '';
  byId('navUser').style.display = 'none';
  bindDashboardEvents();
}

function partialApp(){
  if (!S.session) return;
  renderDashboard();
}

(async()=>{
  const { data:{ session } } = await sb.auth.getSession();
  if (session) {
    S.session = session;
    renderLoadingScreen();
    try {
      await loadData();
    } catch (error) {
      console.error('operativita load error', error);
      toast('Errore caricamento: ' + error.message, 'err');
    }
  } else {
    renderLogin();
  }
})();

sb.auth.onAuthStateChange(async (_, session) => {
  if (!session && S.session) {
    S.session = null;
    S.tasks = [];
    S.selectedId = null;
    S.drawerOpen = false;
    S.form = {};
    renderLogin();
    return;
  }
  if (session && !S.session) {
    S.session = session;
    renderLoadingScreen();
    try { await loadData(); } catch (error) { console.error('operativita auth load error', error); }
  }
});
