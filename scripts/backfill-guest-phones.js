#!/usr/bin/env node
/**
 * Backfill idempotente di bookings.guest_phone per soggiorni futuri senza telefono.
 *
 * Non parte da solo e, senza --execute, non scrive nulla. Non cancella righe e
 * non sovrascrive un guest_phone già presente.
 *
 * Cosa può recuperare
 * - un numero già presente nel raw_payload Beds24 (phone, mobile, guestPhone,
 *   guests[], infoItems, nota con etichetta "Telefono:" / "Phone:")
 * - con --refresh-beds24, lo stesso numero riletto ora da GET /bookings
 *   (includeGuests e includeInfoItems)
 *
 * Cosa non può recuperare
 * Dal 28 settembre 2026 Booking.com non invia più il telefono a Beds24.
 * Per quelle prenotazioni il campo resta vuoto. Il numero è nell'Extranet
 * (prenotazione → Mostra numero) e nell'app Pulse. L'email alias
 * (@guest.booking.com) arriva nella messaggistica Booking.com.
 *
 * Uso (dry-run, default):
 *   SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... \
 *     node scripts/backfill-guest-phones.js
 *
 * Scrittura, solo numeri trovati:
 *   SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... \
 *     node scripts/backfill-guest-phones.js --execute
 *
 * Rilettura Beds24 prima di scrivere (serve anche BEDS24_API_KEY, il refresh token):
 *   SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... BEDS24_API_KEY=... \
 *     node scripts/backfill-guest-phones.js --refresh-beds24 --execute
 *
 * Filtri:
 *   --from=YYYY-MM-DD     check_out da questa data (default: oggi UTC)
 *   --limit=500           massimo di prenotazioni da ispezionare
 *   --property-id=123     solo una property Beds24
 *
 * Non eseguirlo contro produzione da un agent automatico. Lancialo a mano,
 * prima senza --execute, e controlla il riepilogo.
 */

const {
  BOOKING_COM_PHONE_MISSING_REASON,
  extractGuestPhone,
  isBookingComChannel,
  maskPhone,
} = require('../netlify/functions/_lib/guest-phone');

const BEDS24_URL = 'https://api.beds24.com/v2';
const GUEST_STATUSES = ['confirmed', 'new', 'request', 'inquiry', 'unknown'];

function printHelp() {
  console.log(`
Backfill guest_phone per prenotazioni con check-out futuro e telefono vuoto.
Default: dry-run. Aggiungi --execute per scrivere. Vedi il commento in testa al file.
`);
}

function parseArgs(argv) {
  const opts = {
    help: false,
    execute: false,
    refreshBeds24: false,
    from: null,
    limit: 500,
    propertyId: null,
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') opts.help = true;
    else if (arg === '--execute') opts.execute = true;
    else if (arg === '--refresh-beds24') opts.refreshBeds24 = true;
    else if (arg.startsWith('--from=')) opts.from = arg.slice('--from='.length);
    else if (arg.startsWith('--limit=')) opts.limit = Number(arg.slice('--limit='.length));
    else if (arg.startsWith('--property-id=')) opts.propertyId = arg.slice('--property-id='.length).trim();
    else throw new Error(`Argomento sconosciuto: ${arg}`);
  }

  if (!Number.isInteger(opts.limit) || opts.limit < 1 || opts.limit > 5000) {
    throw new Error('--limit deve essere un intero tra 1 e 5000');
  }
  if (opts.from && !/^\d{4}-\d{2}-\d{2}$/.test(opts.from)) {
    throw new Error('--from deve essere YYYY-MM-DD');
  }
  return opts;
}

function todayUtc() {
  return new Date().toISOString().slice(0, 10);
}

function requireEnv(name, aliases) {
  for (const key of [name, ...(aliases || [])]) {
    const value = String(process.env[key] || '').trim();
    if (value) return value;
  }
  throw new Error(`Variabile mancante: ${[name, ...(aliases || [])].join(' oppure ')}`);
}

async function getBeds24AccessToken(apiKey) {
  const res = await fetch(`${BEDS24_URL}/authentication/token`, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      refreshToken: apiKey,
    },
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Autenticazione Beds24 fallita (${res.status})`);
  }
  const payload = JSON.parse(text);
  const token = String(payload?.token || payload?.data?.token || '').trim();
  if (!token) throw new Error('Token Beds24 assente');
  return token;
}

async function fetchBeds24Booking(token, bookingId) {
  const params = new URLSearchParams({
    id: String(bookingId),
    includeInvoice: 'false',
    includeInfoItems: 'true',
    includeGuests: 'true',
  });
  let res = await fetch(`${BEDS24_URL}/bookings?${params.toString()}`, {
    method: 'GET',
    headers: { Accept: 'application/json', token },
  });
  if (res.status === 400 || res.status === 403) {
    params.delete('includeInfoItems');
    params.delete('includeGuests');
    res = await fetch(`${BEDS24_URL}/bookings?${params.toString()}`, {
      method: 'GET',
      headers: { Accept: 'application/json', token },
    });
  }
  if (!res.ok) {
    throw new Error(`Beds24 error ${res.status} per booking ${bookingId}`);
  }
  const payload = await res.json();
  const rows = Array.isArray(payload?.data) ? payload.data : (Array.isArray(payload) ? payload : []);
  return rows[0] || null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadCandidates(supabase, { from, limit, propertyId }) {
  const pageSize = 200;
  const rows = [];
  let offset = 0;

  while (rows.length < limit) {
    const end = Math.min(offset + pageSize, limit) - 1;
    let query = supabase
      .from('bookings')
      .select('id,beds24_booking_id,beds24_property_id,guest_phone,guest_email,channel,channel_normalized,check_in,check_out,status,raw_payload')
      .gte('check_out', from)
      .in('status', GUEST_STATUSES)
      .or('guest_phone.is.null,guest_phone.eq.')
      .order('check_out', { ascending: true })
      .range(offset, end);

    if (propertyId) query = query.eq('beds24_property_id', propertyId);

    const { data, error } = await query;
    if (error) throw new Error(`Lettura bookings fallita: ${error.message}`);
    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);
    if (batch.length < (end - offset + 1)) break;
    offset += pageSize;
  }

  return rows.filter((row) => !String(row.guest_phone || '').trim()).slice(0, limit);
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    printHelp();
    return;
  }

  const from = opts.from || todayUtc();
  const { createClient } = require('@supabase/supabase-js');
  const supabaseUrl = requireEnv('SUPABASE_URL', ['SUPABASE_RUNTIME_URL']);
  const serviceKey = requireEnv('SUPABASE_SERVICE_ROLE_KEY', ['SUPABASE_SERVICE_KEY']);
  const beds24Key = opts.refreshBeds24 ? requireEnv('BEDS24_API_KEY') : null;

  const supabase = createClient(supabaseUrl, serviceKey);
  const candidates = await loadCandidates(supabase, {
    from,
    limit: opts.limit,
    propertyId: opts.propertyId,
  });

  const summary = {
    mode: opts.execute ? 'execute' : 'dry-run',
    from,
    refresh_beds24: opts.refreshBeds24,
    inspected: candidates.length,
    would_update: 0,
    updated: 0,
    still_missing_booking_com: 0,
    still_missing_other: 0,
    errors: 0,
  };
  const samples = [];

  let beds24Token = null;
  if (opts.refreshBeds24) beds24Token = await getBeds24AccessToken(beds24Key);

  for (const row of candidates) {
    try {
      let sourcePayload = row.raw_payload || {};
      let found = extractGuestPhone(sourcePayload);

      if (!found.phone && opts.refreshBeds24 && row.beds24_booking_id) {
        const fresh = await fetchBeds24Booking(beds24Token, row.beds24_booking_id);
        if (fresh) {
          found = extractGuestPhone(fresh);
          if (found.phone) sourcePayload = fresh;
        }
        await sleep(200);
      }

      const bookingCom = isBookingComChannel(row.channel, row.channel_normalized);
      if (!found.phone) {
        if (bookingCom) summary.still_missing_booking_com += 1;
        else summary.still_missing_other += 1;
        if (samples.length < 15) {
          samples.push({
            beds24_booking_id: row.beds24_booking_id,
            check_in: row.check_in,
            check_out: row.check_out,
            channel: row.channel_normalized || row.channel || null,
            action: 'unchanged',
            missing_reason: bookingCom ? BOOKING_COM_PHONE_MISSING_REASON : 'phone_not_in_beds24_payload',
          });
        }
        continue;
      }

      summary.would_update += 1;
      if (samples.length < 15) {
        samples.push({
          beds24_booking_id: row.beds24_booking_id,
          check_in: row.check_in,
          check_out: row.check_out,
          channel: row.channel_normalized || row.channel || null,
          action: opts.execute ? 'update' : 'would_update',
          source: found.source,
          phone_masked: maskPhone(found.phone),
        });
      }

      if (!opts.execute) continue;

      const nextRaw = {
        ...(row.raw_payload || {}),
        __guest_phone: {
          source: found.source,
          missing_reason: null,
          backfilled_at: new Date().toISOString(),
        },
      };
      if (sourcePayload !== row.raw_payload && sourcePayload && typeof sourcePayload === 'object') {
        nextRaw.phone = nextRaw.phone || sourcePayload.phone || null;
        nextRaw.mobile = nextRaw.mobile || sourcePayload.mobile || null;
        if (!nextRaw.guests && sourcePayload.guests) nextRaw.guests = sourcePayload.guests;
        if (!nextRaw.infoItems && sourcePayload.infoItems) nextRaw.infoItems = sourcePayload.infoItems;
      }

      const { data: updated, error } = await supabase
        .from('bookings')
        .update({
          guest_phone: found.phone,
          raw_payload: nextRaw,
        })
        .eq('id', row.id)
        .or('guest_phone.is.null,guest_phone.eq.')
        .select('id');

      if (error) throw new Error(error.message);
      if (Array.isArray(updated) && updated.length) summary.updated += 1;
    } catch (error) {
      summary.errors += 1;
      console.error(`[errore] booking ${row.beds24_booking_id}: ${error.message}`);
    }
  }

  console.log(JSON.stringify({ summary, samples }, null, 2));
  console.log([
    '',
    'Booking.com dal 28 settembre 2026 non include il telefono nel payload verso Beds24.',
    'Il backfill non inventa quei numeri: restano vuoti.',
    'Per contattare l\'ospite: email alias (@guest.booking.com) oppure Extranet / app Pulse → Mostra numero.',
    opts.execute
      ? `Scritture effettive: ${summary.updated}.`
      : 'Dry-run: nessuna riga è stata modificata. Aggiungi --execute per scrivere solo i numeri trovati.',
  ].join('\n'));

  if (summary.errors) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
