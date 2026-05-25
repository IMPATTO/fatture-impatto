const rotationEntries = require('../../reports/security/2026-05-10-public-checkin-key-rotation.json');

const legacyToCurrentKeyMap = new Map(
  (Array.isArray(rotationEntries) ? rotationEntries : [])
    .map((entry) => {
      const legacyKey = normalizeCheckinKey(entry?.old_public_checkin_key);
      const currentKey = String(entry?.new_public_checkin_key || '').trim();
      if (!legacyKey || !currentKey) return null;
      return [legacyKey, currentKey];
    })
    .filter(Boolean)
);

function normalizeCheckinKey(value) {
  return String(value || '').trim().toLowerCase();
}

function resolvePublicCheckinKeyAlias(value) {
  const rawValue = String(value || '').trim();
  if (!rawValue) return '';
  const normalized = normalizeCheckinKey(rawValue);
  return legacyToCurrentKeyMap.get(normalized) || rawValue;
}

module.exports = {
  legacyToCurrentKeyMap,
  normalizeCheckinKey,
  resolvePublicCheckinKeyAlias,
};
