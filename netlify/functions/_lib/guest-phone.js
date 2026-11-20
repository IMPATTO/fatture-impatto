/**
 * Guest phone extraction for Beds24 bookings.
 *
 * Booking.com, from 28 September 2026, omits the guest telephone from every
 * reservation payload sent to connectivity partners (Beds24 included). Those
 * bookings arrive with phone/mobile empty. The number is still visible in the
 * Booking.com Extranet and the Pulse app. Airbnb continues to send a phone.
 *
 * This helper reads every field Beds24 may still populate (header, guests[],
 * infoItems, a labeled note) and ignores empty strings so `phone: ""` does
 * not hide a number stored in `mobile` or on a guest record.
 */

const BOOKING_COM_PHONE_POLICY_DATE = '2026-09-28';
const BOOKING_COM_PHONE_MISSING_REASON = 'booking_com_omits_phone_since_2026-09-28';

const DIRECT_PHONE_KEYS = [
  'phone',
  'mobile',
  'guestPhone',
  'guestMobile',
  'telephone',
  'guest_phone',
  'guest_mobile',
  'tel',
];

const NESTED_CONTACT_KEYS = ['guest', 'customer', 'contact', 'booker', 'primaryGuest'];
const GUEST_LIST_KEYS = ['guests', 'guestDetails', 'additionalGuests'];

const LABELED_TEXT_KEYS = ['comments', 'notes', 'message', 'apiMessage', 'guestComments'];
const LABELED_PHONE_PATTERN = /(?:phone|mobile|telefono|tel|cellulare)\s*[:\-]\s*([+\d][\d\s()./+-]{6,})/ig;

function normalizePhoneCandidate(value) {
  const text = String(value ?? '').trim();
  if (!text) return null;
  if (/^(n\/?a|none|null|undefined|-+|\.+)$/i.test(text)) return null;

  const letters = text.replace(/[^a-zA-Z]/g, '');
  if (letters.length > 4) return null;

  const digits = text.replace(/\D/g, '');
  if (digits.length < 8 || digits.length > 18) return null;
  if (/^0+$/.test(digits)) return null;
  return text;
}

function phoneFromObject(value, sourcePrefix) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  for (const key of DIRECT_PHONE_KEYS) {
    const phone = normalizePhoneCandidate(value[key]);
    if (phone) return { phone, source: `${sourcePrefix}.${key}` };
  }
  return null;
}

function phoneFromInfoItems(raw) {
  const items = Array.isArray(raw?.infoItems)
    ? raw.infoItems
    : (Array.isArray(raw?.info_items) ? raw.info_items : []);

  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    const code = String(item.code || item.name || item.type || '');
    if (!/phone|mobile|tel|cell|telefono/i.test(code)) continue;
    const phone = normalizePhoneCandidate(item.text || item.value || item.content);
    if (phone) return { phone, source: `infoItems:${code}` };
  }
  return null;
}

function phoneFromLabeledText(raw) {
  for (const key of LABELED_TEXT_KEYS) {
    const text = raw?.[key];
    if (typeof text !== 'string' || !text.trim()) continue;
    LABELED_PHONE_PATTERN.lastIndex = 0;
    const match = LABELED_PHONE_PATTERN.exec(text);
    if (!match) continue;
    const phone = normalizePhoneCandidate(match[1]);
    if (phone) return { phone, source: `${key}:label` };
  }
  return null;
}

function extractGuestPhone(rawBooking) {
  if (!rawBooking || typeof rawBooking !== 'object') {
    return { phone: null, source: null };
  }

  const direct = phoneFromObject(rawBooking, 'booking');
  if (direct) return direct;

  for (const key of NESTED_CONTACT_KEYS) {
    const nested = phoneFromObject(rawBooking[key], key);
    if (nested) return nested;
  }

  for (const key of GUEST_LIST_KEYS) {
    const list = rawBooking[key];
    if (!Array.isArray(list)) continue;
    for (let index = 0; index < list.length; index += 1) {
      const nested = phoneFromObject(list[index], `${key}[${index}]`);
      if (nested) return nested;
    }
  }

  return phoneFromInfoItems(rawBooking) || phoneFromLabeledText(rawBooking) || { phone: null, source: null };
}

function isBookingComChannel(channel, channelNormalized) {
  const normalized = String(channelNormalized || '').trim().toLowerCase();
  if (normalized === 'booking' || normalized.includes('booking')) return true;
  return String(channel || '').trim().toLowerCase().includes('booking');
}

function resolveStoredGuestPhone({ rawBooking, existingPhone, channel, channelNormalized }) {
  const extracted = extractGuestPhone(rawBooking);
  if (extracted.phone) {
    return {
      phone: extracted.phone,
      source: extracted.source,
      missing_reason: null,
    };
  }

  const kept = normalizePhoneCandidate(existingPhone) || String(existingPhone ?? '').trim() || null;
  if (kept && String(existingPhone ?? '').trim()) {
    return {
      phone: String(existingPhone).trim(),
      source: 'preserved_existing',
      missing_reason: null,
    };
  }

  const bookingCom = isBookingComChannel(channel, channelNormalized);
  return {
    phone: null,
    source: null,
    missing_reason: bookingCom ? BOOKING_COM_PHONE_MISSING_REASON : null,
  };
}

function maskPhone(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.length < 4) return '***';
  return `***${digits.slice(-4)}`;
}

module.exports = {
  BOOKING_COM_PHONE_POLICY_DATE,
  BOOKING_COM_PHONE_MISSING_REASON,
  extractGuestPhone,
  isBookingComChannel,
  resolveStoredGuestPhone,
  maskPhone,
};
