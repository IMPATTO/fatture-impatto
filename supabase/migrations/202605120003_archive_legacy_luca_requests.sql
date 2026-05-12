-- ============================================================
-- Archive legacy Luca partner booking requests
-- ============================================================
-- Scopo:
--   chiudere in modo esplicito la coda storica PR Luca dopo la
--   dismissione del canale, senza cancellare i dati esistenti.
--
-- Cosa fa:
--   - marca come cancelled tutte le richieste Luca ancora pending/staff
--   - imposta sync_status a not_required
--   - annota il motivo di archiviazione nel sync_error
--   - valorizza approved_at quando manca, per fissare la chiusura operativa
--
-- Cosa NON fa:
--   - non tocca richieste gia confirmed, rejected o cancelled
--   - non modifica le prenotazioni storiche in rm_bookings
-- ============================================================

UPDATE public.partner_booking_requests
SET
  status = 'cancelled',
  sync_status = 'not_required',
  sync_error = COALESCE(
    NULLIF(sync_error, ''),
    'Coda legacy Luca archiviata il 2026-05-12: canale dismesso e nuove richieste non piu gestite.'
  ),
  approved_at = COALESCE(approved_at, now()),
  updated_at = now()
WHERE partner = 'luca'
  AND status IN ('pending', 'staff');
