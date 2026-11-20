const assert = require('assert');
const {
  BOOKING_COM_PHONE_MISSING_REASON,
  extractGuestPhone,
  isBookingComChannel,
  resolveStoredGuestPhone,
} = require('../guest-phone');

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

test('usa phone quando è valorizzato', () => {
  const found = extractGuestPhone({ phone: '+39 333 1234567', mobile: '' });
  assert.strictEqual(found.phone, '+39 333 1234567');
  assert.strictEqual(found.source, 'booking.phone');
});

test('phone vuoto non nasconde mobile', () => {
  const found = extractGuestPhone({ phone: '', mobile: '+393331112233' });
  assert.strictEqual(found.phone, '+393331112233');
  assert.strictEqual(found.source, 'booking.mobile');
});

test('legge guestPhone e il telefono dentro guests[]', () => {
  assert.strictEqual(extractGuestPhone({ guestPhone: '3331234567' }).source, 'booking.guestPhone');
  const fromGuest = extractGuestPhone({
    phone: '',
    mobile: null,
    guests: [{ firstName: 'Ada', mobile: '+1 415 555 0133' }],
  });
  assert.strictEqual(fromGuest.phone, '+1 415 555 0133');
  assert.strictEqual(fromGuest.source, 'guests[0].mobile');
});

test('legge un infoItem il cui codice parla di telefono', () => {
  const found = extractGuestPhone({
    phone: '',
    infoItems: [
      { code: 'CHECKIN', text: 'cassetta 1234' },
      { code: 'GUEST_PHONE', text: '+39 347 1234567' },
    ],
  });
  assert.strictEqual(found.phone, '+39 347 1234567');
  assert.strictEqual(found.source, 'infoItems:GUEST_PHONE');
});

test('legge solo note con etichetta telefono, non un numero qualsiasi', () => {
  const labeled = extractGuestPhone({
    phone: '',
    notes: 'VIP\nTelefono: +39 320 1234567',
  });
  assert.strictEqual(labeled.phone, '+39 320 1234567');
  assert.strictEqual(labeled.source, 'notes:label');

  const unlabeled = extractGuestPhone({
    phone: '',
    notes: 'prenotazione 123456789 arrivo tardi',
  });
  assert.strictEqual(unlabeled.phone, null);
});

test('scarta valori vuoti o troppo corti', () => {
  assert.strictEqual(extractGuestPhone({ phone: '', mobile: 'n/a', guestPhone: '123' }).phone, null);
  assert.strictEqual(extractGuestPhone({ phone: '00000000' }).phone, null);
});

test('una prenotazione Booking.com senza numero resta vuota e spiega il motivo', () => {
  const resolved = resolveStoredGuestPhone({
    rawBooking: { phone: '', mobile: '', referer: 'Booking.com' },
    existingPhone: null,
    channel: 'Booking.com',
    channelNormalized: 'booking',
  });
  assert.strictEqual(resolved.phone, null);
  assert.strictEqual(resolved.missing_reason, BOOKING_COM_PHONE_MISSING_REASON);
  assert.strictEqual(isBookingComChannel('Booking.com', 'booking'), true);
  assert.strictEqual(isBookingComChannel('Airbnb', 'airbnb'), false);
});

test('non cancella un telefono già salvato se Beds24 ora manda il campo vuoto', () => {
  const resolved = resolveStoredGuestPhone({
    rawBooking: { phone: '', mobile: '', apiSource: 'Booking.com' },
    existingPhone: '+39 333 9998877',
    channel: 'Booking.com',
    channelNormalized: 'booking',
  });
  assert.strictEqual(resolved.phone, '+39 333 9998877');
  assert.strictEqual(resolved.source, 'preserved_existing');
  assert.strictEqual(resolved.missing_reason, null);
});

test('un canale diverso senza numero non usa la motivazione Booking.com', () => {
  const resolved = resolveStoredGuestPhone({
    rawBooking: { phone: '', notes: 'arrivo dopo le 21' },
    existingPhone: '   ',
    channel: 'Direct',
    channelNormalized: 'direct',
  });
  assert.strictEqual(resolved.phone, null);
  assert.strictEqual(resolved.source, null);
  assert.strictEqual(resolved.missing_reason, null);
});

test('Airbnb continua a usare il phone di testata', () => {
  const resolved = resolveStoredGuestPhone({
    rawBooking: { phone: '+44 7700 900123', referer: 'Airbnb' },
    existingPhone: null,
    channel: 'Airbnb',
    channelNormalized: 'airbnb',
  });
  assert.strictEqual(resolved.phone, '+44 7700 900123');
  assert.strictEqual(resolved.source, 'booking.phone');
  assert.strictEqual(resolved.missing_reason, null);
});

(async () => {
  let passed = 0;
  let failed = 0;
  for (const current of tests) {
    try {
      await current.fn();
      console.log(`[PASS] ${current.name}`);
      passed += 1;
    } catch (error) {
      console.error(`[FAIL] ${current.name}`);
      console.error(error.stack || error.message);
      failed += 1;
    }
  }
  console.log(`\n${passed} passati, ${failed} falliti`);
  process.exit(failed ? 1 : 0);
})();
