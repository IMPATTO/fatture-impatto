-- ============================================================
-- RESIDENCE MARGHERITA — SETUP SUPABASE
-- Eseguire nel SQL Editor del progetto tysxeikqbgebpfyblgeb
-- Tutte le tabelle hanno prefisso "rm_" per non confondersi
-- con quelle esistenti del progetto fatture-impatto
-- ============================================================

-- ============================================================
-- TABELLA 1: rm_apartments — master appartamenti
-- ============================================================
CREATE TABLE IF NOT EXISTS rm_apartments (
  id text PRIMARY KEY,
  name text NOT NULL,
  type text NOT NULL CHECK (type IN ('mono','bilo','trilo')),
  floor text NOT NULL,
  capacity int NOT NULL CHECK (capacity > 0),
  group_name text NOT NULL,
  note text,
  blocked boolean DEFAULT false,
  external boolean DEFAULT false,
  sort_order int DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

-- ============================================================
-- TABELLA 2: rm_unavailability — finestre indisponibilità
-- (per appartamenti esterni con disponibilità irregolare)
-- ============================================================
CREATE TABLE IF NOT EXISTS rm_unavailability (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  apartment_id text NOT NULL REFERENCES rm_apartments(id) ON DELETE CASCADE,
  start_date date NOT NULL,
  end_date date NOT NULL,
  label text DEFAULT 'Non disponibile',
  created_at timestamptz DEFAULT now(),
  CHECK (end_date > start_date)
);
CREATE INDEX IF NOT EXISTS idx_rm_unavail_apt ON rm_unavailability(apartment_id);
CREATE INDEX IF NOT EXISTS idx_rm_unavail_dates ON rm_unavailability(start_date, end_date);

-- ============================================================
-- TABELLA 3: rm_bookings — prenotazioni
-- ============================================================
CREATE TABLE IF NOT EXISTS rm_bookings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  apartment_id text NOT NULL REFERENCES rm_apartments(id),
  group_id text,                                   -- per split: stesso group_id su 2 segmenti
  segment text,                                    -- '1/2' o '2/2' o NULL
  name text NOT NULL,
  checkin date NOT NULL,
  checkout date NOT NULL,
  pax int NOT NULL CHECK (pax > 0),
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','confirmed','rejected','staff','cancelled')),
  source text NOT NULL
    CHECK (source IN ('checco','pr-serena','pr-luca','esterno','system')),
  notes text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  approved_at timestamptz,
  approved_by text,
  CHECK (checkout > checkin)
);
CREATE INDEX IF NOT EXISTS idx_rm_bookings_apt ON rm_bookings(apartment_id);
CREATE INDEX IF NOT EXISTS idx_rm_bookings_dates ON rm_bookings(checkin, checkout);
CREATE INDEX IF NOT EXISTS idx_rm_bookings_status ON rm_bookings(status);
CREATE INDEX IF NOT EXISTS idx_rm_bookings_source ON rm_bookings(source);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION rm_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS rm_bookings_updated_at ON rm_bookings;
CREATE TRIGGER rm_bookings_updated_at
  BEFORE UPDATE ON rm_bookings
  FOR EACH ROW EXECUTE FUNCTION rm_update_updated_at();

-- ============================================================
-- TABELLA 4: rm_audit — log azioni admin (chi ha approvato cosa)
-- ============================================================
CREATE TABLE IF NOT EXISTS rm_audit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  action text NOT NULL,                            -- 'create','approve','reject','modify','delete'
  booking_id uuid,
  actor text NOT NULL,                             -- 'kekko' | 'admin'
  details jsonb,
  created_at timestamptz DEFAULT now()
);

-- ============================================================
-- RLS — DISABILITATA per queste tabelle (auth via password lato app)
-- L'anon key di Supabase può leggere/scrivere tutto.
-- Sicurezza garantita dalla password condivisa nel frontend.
-- ============================================================
ALTER TABLE rm_apartments DISABLE ROW LEVEL SECURITY;
ALTER TABLE rm_unavailability DISABLE ROW LEVEL SECURITY;
ALTER TABLE rm_bookings DISABLE ROW LEVEL SECURITY;
ALTER TABLE rm_audit DISABLE ROW LEVEL SECURITY;

-- ============================================================
-- SEED DATI: 19 APPARTAMENTI + 3 ESTERNI
-- ============================================================
INSERT INTO rm_apartments (id, name, type, floor, capacity, group_name, note, blocked, external, sort_order) VALUES
  -- 4 MONO
  ('M1', '104', 'mono', 'PT', 3, 'Monolocali', 'il migliore', false, false, 1),
  ('M2', 'Mono 1° A', 'mono', '1°', 2, 'Monolocali', NULL, false, false, 2),
  ('M3', 'Mono 1° B', 'mono', '1°', 2, 'Monolocali', NULL, false, false, 3),
  ('M4', 'Mono 3°', 'mono', '3°', 2, 'Monolocali', 'standard base', false, false, 4),
  -- 10 BILO
  ('BD1', 'Dependance 1', 'bilo', 'Dep', 5, 'Bilocali · Dependance', NULL, false, false, 10),
  ('BD2', 'Dependance 2', 'bilo', 'Dep', 5, 'Bilocali · Dependance', NULL, false, false, 11),
  ('BPT1', 'Bilo PT 1', 'bilo', 'PT', 6, 'Bilocali · Piano Terra', NULL, false, false, 20),
  ('BPT2', 'Bilo PT 2', 'bilo', 'PT', 6, 'Bilocali · Piano Terra', NULL, false, false, 21),
  ('BPT3', 'Bilo PT 3', 'bilo', 'PT', 6, 'Bilocali · Piano Terra', 'app.5', false, false, 22),
  ('BPT4', 'Bilo PT 4', 'bilo', 'PT', 6, 'Bilocali · Piano Terra', NULL, false, false, 23),
  ('B1A', 'Bilo 1° A', 'bilo', '1°', 6, 'Bilocali · Primo Piano', NULL, false, false, 30),
  ('B1B', 'Bilo 1° B', 'bilo', '1°', 6, 'Bilocali · Primo Piano', NULL, false, false, 31),
  ('B1C', 'Bilo 1° C', 'bilo', '1°', 6, 'Bilocali · Primo Piano', NULL, false, false, 32),
  ('B202', '202 ★', 'bilo', '1°', 7, 'Bilocali · Primo Piano', 'vista mare', false, false, 33),
  -- 5 TRILO (3 operativi + 2 bloccati)
  ('T1', 'Trilo 1° ★', 'trilo', '1°', 6, 'Trilocali', 'vista mare', false, false, 40),
  ('T3A', 'Trilo 3° A', 'trilo', '3°', 8, 'Trilocali', NULL, false, false, 41),
  ('T3B', 'Trilo 3° B', 'trilo', '3°', 8, 'Trilocali', NULL, false, false, 42),
  ('T3C_FAMILY', 'Trilo 3° C', 'trilo', '3°', 8, 'Trilocali', 'Family Hotel · stagione', true, false, 43),
  ('T3D_STAFF', 'Trilo 3° D', 'trilo', '3°', 8, 'Trilocali', 'Staff · stagione', true, false, 44),
  -- 3 ESTERNI
  ('EXT_PORTO', 'Riccione Porto', 'trilo', 'Esterno', 8, 'Esterni · Altri Appartamenti', 'libero dal 20/6', false, true, 50),
  ('EXT_DANTE', 'Viale Dante', 'bilo', 'Esterno', 6, 'Esterni · Altri Appartamenti', 'bloccato 16-30 lug', false, true, 51),
  ('EXT_MARANO', 'Marano', 'bilo', 'Esterno', 7, 'Esterni · Altri Appartamenti', 'finestre frammentate', false, true, 52)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  type = EXCLUDED.type,
  floor = EXCLUDED.floor,
  capacity = EXCLUDED.capacity,
  group_name = EXCLUDED.group_name,
  note = EXCLUDED.note,
  blocked = EXCLUDED.blocked,
  external = EXCLUDED.external,
  sort_order = EXCLUDED.sort_order;

-- ============================================================
-- SEED INDISPONIBILITÀ ESTERNI
-- ============================================================
INSERT INTO rm_unavailability (apartment_id, start_date, end_date, label) VALUES
  -- Riccione Porto: bloccato prima del 20/6
  ('EXT_PORTO', '2026-01-01', '2026-06-20', 'Non disponibile'),
  -- Viale Dante: bloccato 16-30 luglio (incl) → fino al 31/7 escluso
  ('EXT_DANTE', '2026-07-16', '2026-07-31', 'Bloccato'),
  -- Marano: finestre disponibili sono 31/5-21/6, 27/6-3/7, 9-10/7, 17/7-15/9
  ('EXT_MARANO', '2026-01-01', '2026-05-31', 'Non disponibile'),
  ('EXT_MARANO', '2026-06-22', '2026-06-27', 'Non disponibile'),
  ('EXT_MARANO', '2026-07-04', '2026-07-09', 'Non disponibile'),
  ('EXT_MARANO', '2026-07-11', '2026-07-17', 'Non disponibile'),
  ('EXT_MARANO', '2026-09-16', '2026-12-31', 'Non disponibile')
ON CONFLICT DO NOTHING;

-- ============================================================
-- SEED PRENOTAZIONI (piano allocazione v6 completo)
-- ============================================================
INSERT INTO rm_bookings (apartment_id, group_id, segment, name, checkin, checkout, pax, status, source) VALUES
  -- BLOCCHI STAGIONE
  ('T3C_FAMILY', NULL, NULL, 'Family Hotel — bloccato', '2026-05-20', '2026-09-20', 8, 'staff', 'system'),
  ('T3D_STAFF', NULL, NULL, 'Staff — bloccato', '2026-05-20', '2026-09-20', 4, 'staff', 'system'),
  -- GIUGNO – LUGLIO
  ('T3B', NULL, NULL, 'Asia Forestieri', '2026-06-27', '2026-07-04', 6, 'confirmed', 'checco'),
  ('EXT_PORTO', NULL, NULL, 'Antonio', '2026-06-28', '2026-07-05', 8, 'confirmed', 'esterno'),
  ('T3A', NULL, NULL, 'Peluso', '2026-07-03', '2026-07-10', 8, 'confirmed', 'checco'),
  ('BPT1', NULL, NULL, 'Belotti (1/2)', '2026-07-06', '2026-07-12', 6, 'confirmed', 'checco'),
  ('BPT2', NULL, NULL, 'Belotti (2/2)', '2026-07-06', '2026-07-12', 4, 'confirmed', 'checco'),
  ('BD1', NULL, NULL, 'Bollo', '2026-07-06', '2026-07-13', 5, 'confirmed', 'checco'),
  ('EXT_DANTE', NULL, NULL, 'Bartolucci', '2026-07-07', '2026-07-14', 6, 'confirmed', 'checco'),
  ('B202', NULL, NULL, 'Vestentini', '2026-07-08', '2026-07-15', 7, 'confirmed', 'checco'),
  ('T3B', NULL, NULL, 'Gamberoni', '2026-07-08', '2026-07-15', 8, 'confirmed', 'checco'),
  ('EXT_PORTO', NULL, NULL, 'Chiofalo (1/2)', '2026-07-08', '2026-07-15', 7, 'confirmed', 'checco'),
  ('T1', NULL, NULL, 'Chiofalo (2/2)', '2026-07-08', '2026-07-15', 6, 'confirmed', 'checco'),
  ('M1', NULL, NULL, 'Pierre Serena #1', '2026-07-10', '2026-07-20', 2, 'confirmed', 'pr-serena'),
  ('M4', NULL, NULL, 'Luca PR (estate)', '2026-07-13', '2026-08-17', 2, 'confirmed', 'pr-luca'),
  ('T3A', NULL, NULL, 'Salaris (1/2)', '2026-07-12', '2026-07-19', 7, 'confirmed', 'checco'),
  ('B1B', NULL, NULL, 'Salaris (2/2)', '2026-07-12', '2026-07-19', 6, 'confirmed', 'checco'),
  ('T3B', NULL, NULL, 'Bregolin (1/2)', '2026-07-15', '2026-07-22', 6, 'confirmed', 'checco'),
  ('BPT1', NULL, NULL, 'Bregolin (2/2)', '2026-07-15', '2026-07-22', 4, 'confirmed', 'checco'),
  ('BD2', NULL, NULL, 'Ruffoni', '2026-07-16', '2026-07-23', 5, 'confirmed', 'checco'),
  ('B202', NULL, NULL, 'Beatrice', '2026-07-17', '2026-07-24', 7, 'confirmed', 'checco'),
  ('BD1', NULL, NULL, 'Andrea Arduino', '2026-07-18', '2026-07-25', 5, 'confirmed', 'checco'),
  ('EXT_MARANO', NULL, NULL, 'Zaruso', '2026-07-19', '2026-07-26', 7, 'confirmed', 'checco'),
  ('BPT2', NULL, NULL, 'Asia Giraldo', '2026-07-19', '2026-07-26', 5, 'confirmed', 'checco'),
  ('EXT_PORTO', NULL, NULL, 'Mazzolini', '2026-07-20', '2026-07-27', 6, 'confirmed', 'checco'),
  ('B1A', NULL, NULL, 'Stella', '2026-07-20', '2026-07-27', 6, 'confirmed', 'checco'),
  ('BPT3', NULL, NULL, 'Chiapparini app.5', '2026-07-20', '2026-07-27', 5, 'confirmed', 'checco'),
  ('BPT4', NULL, NULL, 'Sara Di Mastro', '2026-07-20', '2026-07-27', 4, 'confirmed', 'checco'),
  ('M2', NULL, NULL, 'Pierre Serena #2', '2026-07-20', '2026-07-27', 3, 'confirmed', 'pr-serena'),
  ('M3', NULL, NULL, 'Pierre Serena PR', '2026-07-20', '2026-07-27', 3, 'confirmed', 'pr-serena'),
  ('B1C', NULL, NULL, 'Morvasa Monti', '2026-07-21', '2026-07-28', 4, 'confirmed', 'checco'),
  ('T1', NULL, NULL, 'Anderle', '2026-07-22', '2026-07-29', 5, 'confirmed', 'checco'),
  ('BD1', NULL, NULL, 'Vinci (1/2)', '2026-07-25', '2026-08-01', 5, 'confirmed', 'checco'),
  ('BD2', NULL, NULL, 'Vinci (2/2)', '2026-07-25', '2026-08-01', 4, 'confirmed', 'checco'),
  ('BPT1', NULL, NULL, 'Cosenza', '2026-07-27', '2026-08-03', 6, 'confirmed', 'checco'),
  ('BPT3', NULL, NULL, 'Brozzetti', '2026-07-28', '2026-08-04', 6, 'confirmed', 'checco'),
  ('BPT2', NULL, NULL, 'Fagiani', '2026-08-01', '2026-08-08', 5, 'confirmed', 'checco'),
  ('BPT4', NULL, NULL, 'Scotton', '2026-08-01', '2026-08-09', 6, 'confirmed', 'checco'),
  ('B1B', NULL, NULL, 'Chiara Osmo', '2026-08-01', '2026-08-08', 5, 'confirmed', 'checco'),
  ('EXT_MARANO', NULL, NULL, 'Greta (1/3)', '2026-08-02', '2026-08-09', 7, 'confirmed', 'checco'),
  ('BD1', NULL, NULL, 'Greta (2/3)', '2026-08-02', '2026-08-09', 5, 'confirmed', 'checco'),
  ('BD2', NULL, NULL, 'Greta (3/3)', '2026-08-02', '2026-08-09', 4, 'confirmed', 'checco'),
  ('B1C', NULL, NULL, 'Migliorati (1/2)', '2026-08-03', '2026-08-10', 5, 'confirmed', 'checco'),
  ('B202', NULL, NULL, 'Migliorati (2/2)', '2026-08-03', '2026-08-10', 4, 'confirmed', 'checco'),
  ('BD2', NULL, NULL, 'Dylan', '2026-08-09', '2026-08-16', 4, 'confirmed', 'checco'),
  ('T1', NULL, NULL, 'Pierre Serena #3', '2026-08-10', '2026-08-20', 5, 'confirmed', 'pr-serena'),
  ('BD1', NULL, NULL, 'Ratti Pazzo', '2026-08-14', '2026-08-21', 4, 'confirmed', 'checco'),
  ('BD2', NULL, NULL, 'Tedesco', '2026-08-17', '2026-08-24', 4, 'confirmed', 'checco'),
  ('EXT_DANTE', NULL, NULL, 'Crescenzio (1/2)', '2026-08-25', '2026-08-31', 6, 'confirmed', 'checco'),
  ('BPT2', NULL, NULL, 'Crescenzio (2/2)', '2026-08-25', '2026-08-31', 6, 'confirmed', 'checco')
ON CONFLICT DO NOTHING;

-- ============================================================
-- VERIFICA SETUP
-- ============================================================
SELECT 'Apartments' as tabella, count(*) as righe FROM rm_apartments
UNION ALL
SELECT 'Unavailability', count(*) FROM rm_unavailability
UNION ALL
SELECT 'Bookings', count(*) FROM rm_bookings;
-- Atteso: 22 apartments, 7 unavailability, 49 bookings
