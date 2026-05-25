const LOCKED_BEDS24_PROPERTIES = new Map([
  ['324753', {
    propertyId: '324753',
    label: 'Villa Margherita',
    reason: 'Sincronizzazione verso Beds24 disattivata finche non viene riattivata manualmente.',
  }],
]);

function getBeds24WriteLock(propertyId) {
  const normalized = String(propertyId || '').trim();
  if (!normalized) return null;
  return LOCKED_BEDS24_PROPERTIES.get(normalized) || null;
}

function buildBeds24WriteLockError(propertyId) {
  const lock = getBeds24WriteLock(propertyId);
  if (!lock) return null;
  return {
    statusCode: 423,
    error: `Scritture Beds24 bloccate per ${lock.label}`,
    detail: lock.reason,
    property_id: lock.propertyId,
    property_label: lock.label,
    lock_code: 'beds24_write_locked',
  };
}

module.exports = {
  LOCKED_BEDS24_PROPERTIES,
  getBeds24WriteLock,
  buildBeds24WriteLockError,
};
