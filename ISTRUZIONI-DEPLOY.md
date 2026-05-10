# 🚀 DEPLOY RESIDENCE MARGHERITA — Istruzioni

## ⚠️ PRIMA DI TUTTO: BACKUP

```bash
cd ~/Desktop/fatture-impatto
git add -A && git commit -m "backup pre-residence-margherita"
```

---

## 📋 STEP 1 — Setup Supabase (5 minuti)

1. Vai su https://supabase.com/dashboard/project/tysxeikqbgebpfyblgeb/sql/new
2. Apri il file `setup-supabase.sql`
3. Copia tutto il contenuto e incollalo nell'editor SQL
4. Clicca **Run** (in basso a destra)
5. Verifica nell'output: dovresti vedere:
   ```
   Apartments: 22 righe
   Unavailability: 7 righe
   Bookings: 50 righe
   ```
6. Se vedi errori del tipo "table already exists" non è un problema (CREATE IF NOT EXISTS è idempotente)

### Trova la tua ANON KEY
1. Vai su https://supabase.com/dashboard/project/tysxeikqbgebpfyblgeb/settings/api
2. Copia il valore di **anon public** (la chiave lunga che inizia con `eyJ...`)
3. Tienila pronta per il prossimo step

---

## 🔑 STEP 2 — Configura le password e l'API key

Apri i 2 file HTML in un editor di testo (TextEdit, VSCode, ecc.) e modifica queste 3 righe in entrambi:

### In `residence-kekko.html` (righe ~600 circa):
```javascript
const SUPABASE_ANON_KEY = 'INSERIRE_QUI_LA_TUA_ANON_KEY';  // ← incolla la chiave anon
const KEKKO_PASSWORD = 'kekko2026';                         // ← cambia con una password che dai a Kekko
```

### In `residence-backoffice.html` (righe ~280 circa):
```javascript
const SUPABASE_ANON_KEY = 'INSERIRE_QUI_LA_TUA_ANON_KEY';  // ← stessa chiave anon
const ADMIN_PASSWORD = 'marco2026';                         // ← cambia con la TUA password admin
```

**Suggerimento password**: usa qualcosa di facile per Kekko (es. `kekko2026!`) e qualcosa di forte per te (es. `Marco$Lupo2026!`).

---

## 📁 STEP 3 — Aggiungi i file al progetto fatture-impatto

```bash
cd ~/Desktop/fatture-impatto
# Copia i 2 file HTML dalla cartella dove li hai scaricati
cp ~/Downloads/residence-kekko.html .
cp ~/Downloads/residence-backoffice.html .
```

---

## 🌐 STEP 4 — Deploy su Netlify esistente

```bash
cd ~/Desktop/fatture-impatto
git add residence-kekko.html residence-backoffice.html
git commit -m "feat: add residence margherita calendar (kekko + backoffice)"
git push
```

Netlify rileva automaticamente il push e deploya. In ~30 secondi i 2 file sono online.

### URL pubblici risultanti:
- **Kekko**: https://checkin.illupoaffitta.com/pr_kekko_riccione.html
- **Tu admin**: https://checkin.illupoaffitta.com/residence-backoffice.html

---

## 🧪 STEP 5 — Test

### Test admin (tu):
1. Apri https://checkin.illupoaffitta.com/residence-backoffice.html
2. Inserisci la password admin
3. Vedi tutte le 50 prenotazioni precaricate
4. Filtra per "Pending" → deve mostrare 0 (sono tutte già confermate dal piano)
5. Crea una prenotazione di test dalla vista Kekko (apri in incognito)
6. Torna sul backoffice → vedi la nuova prenotazione apparire in tempo reale (real-time subscription)
7. Approvala con il bottone verde
8. Torna su Kekko → vedi lo stato cambiato

### Test Kekko:
1. Apri https://checkin.illupoaffitta.com/pr_kekko_riccione.html in incognito
2. Inserisci la password Kekko
3. Vedi il calendario completo
4. Crea una prenotazione test → deve apparire **arancione (pending)**
5. Verifica che NON puoi modificare prenotazioni di altri PR (Pierre Serena, Luca, esterni)

---

## 📲 STEP 6 — Condividi con Kekko

Manda a Kekko:

> Ciao Kekko, ecco il calendario per il Residence Margherita per la stagione 2026:
> 
> 🔗 https://checkin.illupoaffitta.com/pr_kekko_riccione.html
> 
> Password: `kekko2026` (cambia con quella vera)
>
> Quando inserisci una nuova prenotazione resta in stato "in attesa" finché non la confermo io.
> Puoi modificare/eliminare le tue richieste pending. Le prenotazioni già confermate vanno modificate parlando con me.

Per te basta salvarti il backoffice nei preferiti.

---

## 🛠️ MANUTENZIONE

### Cambiare password
Modifica `KEKKO_PASSWORD` o `ADMIN_PASSWORD` nel file HTML, commit + push.

### Aggiungere/modificare appartamenti
Vai su Supabase Dashboard → Table Editor → `rm_apartments` → modifica direttamente.

### Aggiungere finestre di indisponibilità (esterni)
Tabella `rm_unavailability` → aggiungi riga con apartment_id, start_date, end_date.

### Vedere il log delle azioni admin
Tabella `rm_audit` → ogni approvazione/rifiuto/modifica viene loggata con actor + timestamp + dettagli.

### Resettare tutto
```sql
TRUNCATE rm_audit, rm_bookings, rm_unavailability, rm_apartments CASCADE;
```
poi rilancia il SQL di seed.

---

## 🐛 TROUBLESHOOTING

**"Errore caricamento dati: Invalid API key"**
→ Controlla di aver incollato bene l'ANON KEY (deve iniziare con `eyJ`).

**"Errore: new row violates check constraint"**
→ Una data check-out è ≤ check-in. Controlla i dati inseriti.

**Real-time non funziona**
→ Vai su Supabase → Database → Replication → abilita la replication su `rm_bookings`.

**Mac multi-tab Supabase blocca**
→ È il bug noto del Web Locks. Nel file kekko c'è il client semplice, dovrebbe funzionare. Se hai problemi, sostituisci la creazione client con la versione del tuo `js/supabase-client.js`.

---

## 📊 STRUTTURA TABELLE CREATE

| Tabella | Scopo | Righe iniziali |
|---|---|---|
| `rm_apartments` | Master appartamenti (interni + esterni) | 22 |
| `rm_unavailability` | Finestre indisponibilità esterni | 7 |
| `rm_bookings` | Prenotazioni con stato + source | 50 |
| `rm_audit` | Log azioni admin | 0 |

Tutte le tabelle hanno prefisso `rm_` per non interferire con il sistema check-in esistente.

---

## 🚀 PROSSIMI STEP POSSIBILI

Quando vuoi possiamo aggiungere:
- Notifiche email quando Kekko inserisce una richiesta (Netlify Function + Resend)
- Integrazione Octorate per sync calendario
- Vista "disponibilità rapida" per Kekko (cerca date libere senza vedere il calendario)
- Conteggio fatturato per cliente nella vista admin
- Bot WhatsApp per ricevere notifiche pending

Apri quando sei pronto e ne parliamo.
