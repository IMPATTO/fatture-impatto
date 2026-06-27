const GLOBAL_PMS_EDITOR_EMAILS = new Set([
  'fatturazione@illupoaffitta.com',
  'contabilita@illupoaffitta.com',
  'info@marcovenzon.com',
  'veronica.dieta@gmail.com',
  'jessica.appartamenticaldari@gmail.com',
  'cerulliserena@gmail.com',
]);

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeId(value) {
  return String(value || '').trim();
}

async function resolvePmsCalendarScope(supabase, email) {
  const normalizedEmail = normalizeEmail(email);
  const isGlobalEditor = GLOBAL_PMS_EDITOR_EMAILS.has(normalizedEmail);

  const result = {
    email: normalizedEmail,
    isGlobalEditor,
    apartmentIds: [],
    editableApartmentIds: [],
    canViewAny: isGlobalEditor,
    canEditAny: isGlobalEditor,
  };

  if (!supabase || !normalizedEmail) {
    return result;
  }

  const { data, error } = await supabase
    .from('pms_calendar_access')
    .select('apartment_id, can_edit')
    .eq('login_email', normalizedEmail);

  if (error) {
    throw new Error(`pms_calendar_access lookup failed: ${error.message}`);
  }

  const apartmentIds = new Set();
  const editableApartmentIds = new Set();
  for (const row of data || []) {
    const apartmentId = normalizeId(row?.apartment_id);
    if (!apartmentId) continue;
    apartmentIds.add(apartmentId);
    if (row?.can_edit === true) {
      editableApartmentIds.add(apartmentId);
    }
  }

  result.apartmentIds = [...apartmentIds];
  result.editableApartmentIds = [...editableApartmentIds];
  result.canViewAny = result.canViewAny || result.apartmentIds.length > 0;
  result.canEditAny = result.canEditAny || result.editableApartmentIds.length > 0;

  return result;
}

function canViewApartment(scope, apartmentId) {
  const normalizedId = normalizeId(apartmentId);
  if (!normalizedId) return false;
  if (scope?.isGlobalEditor) return true;
  return Array.isArray(scope?.apartmentIds) && scope.apartmentIds.includes(normalizedId);
}

function canEditApartment(scope, apartmentId) {
  const normalizedId = normalizeId(apartmentId);
  if (!normalizedId) return false;
  if (scope?.isGlobalEditor) return true;
  return Array.isArray(scope?.editableApartmentIds) && scope.editableApartmentIds.includes(normalizedId);
}

module.exports = {
  GLOBAL_PMS_EDITOR_EMAILS,
  normalizeEmail,
  normalizeId,
  resolvePmsCalendarScope,
  canViewApartment,
  canEditApartment,
};
