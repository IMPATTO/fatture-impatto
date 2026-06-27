const assert = require('assert');

const {
  buildLocalRowFromSources,
} = require('../beds24-push-calendar').__test__;

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

test('mantiene il min stay richiesto anche se lo snapshot Beds24 torna ancora stantio', () => {
  const row = buildLocalRowFromSources({
    change: {
      apartment_unit_id: 324746,
      min_stay: 1,
    },
    date: '2026-06-27',
    calendarEntry: {
      price: 326,
      minStay: 3,
      closed: false,
    },
    availability: true,
    timestamp: '2026-06-27T10:00:00.000Z',
  });

  assert.deepStrictEqual(row, {
    apartment_unit_id: 324746,
    date: '2026-06-27',
    source_updated_at: '2026-06-27T10:00:00.000Z',
    price: 326,
    min_stay: 1,
    available: true,
    closed: false,
  });
});

test('mantiene la disponibilita richiesta anche se il read-back availability e ancora vecchio', () => {
  const row = buildLocalRowFromSources({
    change: {
      apartment_unit_id: 324746,
      closed: false,
    },
    date: '2026-06-28',
    calendarEntry: {
      price: 326,
      minStay: 3,
      closed: true,
    },
    availability: false,
    timestamp: '2026-06-27T10:00:00.000Z',
  });

  assert.deepStrictEqual(row, {
    apartment_unit_id: 324746,
    date: '2026-06-28',
    source_updated_at: '2026-06-27T10:00:00.000Z',
    price: 326,
    min_stay: 3,
    available: true,
    closed: false,
  });
});

test('mantiene il price richiesto anche se lo snapshot Beds24 torna ancora stantio', () => {
  const row = buildLocalRowFromSources({
    change: {
      apartment_unit_id: 324746,
      price: 150,
    },
    date: '2026-06-29',
    calendarEntry: {
      price: 200,
      minStay: 3,
      closed: false,
    },
    availability: true,
    timestamp: '2026-06-27T10:00:00.000Z',
  });

  assert.deepStrictEqual(row, {
    apartment_unit_id: 324746,
    date: '2026-06-29',
    source_updated_at: '2026-06-27T10:00:00.000Z',
    price: 150,
    min_stay: 3,
    available: true,
    closed: false,
  });
});

test('usa lo snapshot per i campi NON presenti nella request', () => {
  const row = buildLocalRowFromSources({
    change: {
      apartment_unit_id: 324746,
      // solo closed — price e min_stay assenti dalla request
      closed: true,
    },
    date: '2026-06-30',
    calendarEntry: {
      price: 99,
      minStay: 2,
      closed: false,
    },
    availability: true,
    timestamp: '2026-06-27T10:00:00.000Z',
  });

  assert.deepStrictEqual(row, {
    apartment_unit_id: 324746,
    date: '2026-06-30',
    source_updated_at: '2026-06-27T10:00:00.000Z',
    price: 99,
    min_stay: 2,
    closed: true,
    available: false,
  });
});

(async () => {
  let passed = 0;
  let failed = 0;

  for (const currentTest of tests) {
    try {
      await currentTest.fn();
      console.log(`[PASS] ${currentTest.name}`);
      passed += 1;
    } catch (error) {
      console.error(`[FAIL] ${currentTest.name}`);
      console.error(error.stack || error.message);
      failed += 1;
    }
  }

  console.log(`\n${passed} passati, ${failed} falliti`);
  process.exit(failed ? 1 : 0);
})();
