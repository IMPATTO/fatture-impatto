# Processo Progetto: App Mail Interna Aziendale

## Obiettivo
Costruire una web app interna, usabile da te e da Veronica da PC diversi, per gestire in un'unica interfaccia le caselle email aziendali come `info@villamargheritarimini.com` e altre caselle future.

L'app non deve essere un clone completo di Spark. Deve coprire molto bene il vostro flusso reale:

- inbox unificata
- accesso multiutente
- lettura e risposta email
- assegnazione interna
- note operative
- filtri per struttura/argomento
- audit minimo delle attività

## Utenti
- Admin: tu
- Operativa: Veronica
- Futuri utenti interni: eventuali collaboratori con permessi limitati

## Risultato atteso V1
Una web app interna accessibile via browser con login, dove due utenti possono:

- vedere le email delle caselle collegate
- leggere thread e allegati
- rispondere dalla casella corretta
- assegnare una conversazione a una persona
- segnare stato operativo (`nuova`, `in lavorazione`, `in attesa`, `chiusa`)
- lasciare note interne
- cercare e filtrare

## Non obiettivi iniziali
Da non fare nella V1:

- app mobile nativa
- editor collaborativo tipo Google Docs
- sincronizzazione offline complessa
- supporto POP3
- calendario integrato
- AI avanzata di risposta automatica
- shared inbox “enterprise” con feature complesse

## Architettura consigliata
### Frontend
- React + Vite
- UI interna semplice e veloce
- layout desktop-first, responsive per uso mobile occasionale

### Backend
- Node.js
- API server interno
- coda per sync email in background

### Database
- Supabase/Postgres

### Email
- IMAP per lettura e sincronizzazione
- SMTP per invio
- per provider moderni: supporto OAuth in fase 2
- per Aruba / caselle tradizionali: IMAP/SMTP classico in V1

### Hosting
- Web app su Netlify o app server dedicato
- worker/sync email su funzione schedulata o servizio server persistente

## Scelta tecnica pratica
Per una inbox seria, io consiglio:

- frontend separato
- backend separato
- sync email gestita dal backend

Non consiglio di leggere IMAP direttamente dal browser.

## Flusso di prodotto
### 1. Collegamento casella
Admin inserisce:
- email
- provider
- host IMAP
- porta IMAP
- host SMTP
- porta SMTP
- username
- password o credenziali app

Il sistema salva le credenziali cifrate e prova connessione.

### 2. Sync iniziale
Il backend:
- legge cartelle principali
- importa ultimi N giorni/thread
- salva mittente, destinatari, subject, snippet, body normalizzato, flags, allegati metadata

### 3. Aggiornamento continuo
Due opzioni:
- polling ogni 1-5 minuti in V1
- IMAP IDLE in fase 2

### 4. Gestione operativa
Ogni conversazione può avere:
- owner
- stato
- tag
- note interne
- collegamento a struttura

### 5. Invio risposta
L'utente apre il thread, risponde, il backend invia via SMTP con la casella giusta e salva evento e metadati.

## Modello dati minimo
### `users`
- id
- email
- full_name
- role
- active
- created_at

### `mailboxes`
- id
- email_address
- display_name
- provider
- imap_host
- imap_port
- imap_secure
- smtp_host
- smtp_port
- smtp_secure
- username
- encrypted_secret
- status
- last_sync_at
- created_at

### `mailbox_access`
- id
- mailbox_id
- user_id
- permission (`admin`, `operator`, `read_only`)

### `conversations`
- id
- mailbox_id
- external_thread_key
- subject
- normalized_subject
- last_message_at
- status
- assigned_user_id
- priority
- structure_key
- created_at
- updated_at

### `messages`
- id
- conversation_id
- external_message_id
- direction (`inbound`, `outbound`)
- from_name
- from_email
- to_json
- cc_json
- bcc_json
- subject
- text_body
- html_body
- snippet
- sent_at
- received_at
- is_read
- has_attachments
- raw_headers_json

### `attachments`
- id
- message_id
- filename
- mime_type
- byte_size
- storage_path

### `conversation_notes`
- id
- conversation_id
- user_id
- body
- created_at

### `conversation_tags`
- id
- conversation_id
- tag

### `activity_log`
- id
- actor_user_id
- mailbox_id
- conversation_id
- action
- metadata_json
- created_at

## Permessi
### Admin
- collega caselle
- modifica credenziali
- assegna accessi
- vede tutto

### Operativa
- legge
- risponde
- assegna a sé
- cambia stato
- inserisce note

### Read only
- solo lettura

## Sicurezza
### Obbligatorio
- credenziali caselle cifrate a riposo
- accesso app con login individuale
- audit log minimo
- HTTPS ovunque
- backup database
- protezione CSRF/sessioni
- separazione permessi admin/operator

### Consigliato
- usare password app dedicate dove possibile
- non condividere password della casella tra utenti finali
- ruotare credenziali delle caselle se vengono esposte

## UX V1
### Schermata 1: Inbox
- sidebar con caselle
- filtri
- lista conversazioni
- badge stato
- owner
- struttura

### Schermata 2: Thread
- storico messaggi
- allegati
- note interne laterali
- azioni rapide

### Schermata 3: Composer
- reply / forward
- selezione mittente
- bozza

### Schermata 4: Admin
- aggiungi casella
- test connessione
- gestione utenti
- permessi

## Workflow operativo consigliato
### Nuova email
1. entra in inbox
2. sistema crea o aggiorna thread
3. stato `nuova`
4. utente assegna a sé o ad altra persona

### Email in lavorazione
1. owner apre thread
2. aggiunge nota se serve
3. risponde
4. stato `in attesa` o `chiusa`

### Email senza risposta
Possibile fase 2:
- reminder se thread senza owner o senza risposta entro X ore

## Roadmap
### Fase 0: Discovery tecnica
- scegliere prima casella da integrare
- test IMAP/SMTP su `info@villamargheritarimini.com`
- definire hosting backend
- definire ruoli utenti

### Fase 1: MVP operativo
- auth utenti
- collegamento 1 casella
- sync inbox
- lista conversazioni
- dettaglio thread
- invio risposta
- note
- assegnazione
- stati

### Fase 2: Multi-casella
- più mailbox
- inbox unificata
- filtri per casella e struttura
- tagging
- allegati salvati

### Fase 3: Operatività avanzata
- template risposte
- SLA base
- reminder
- ricerca full text
- collegamento con clienti/strutture

### Fase 4: AI utile, non invasiva
- riassunto thread
- suggerimento bozza
- classificazione automatica
- estrazione dati strutturati

## Stima realistica
### MVP serio
- 1 sviluppatore forte: circa 2-4 settimane
- con rifiniture, test e deploy pulito: 4-6 settimane

### Clone completo di Spark
- molto più grande, non consigliato

## Decisioni consigliate
### Scelte da fare subito
- partire con una sola casella: `info@villamargheritarimini.com`
- utenti iniziali: tu + Veronica
- interfaccia web interna
- IMAP/SMTP classico

### Scelte da rimandare
- multi-provider avanzato
- mobile app
- AI
- automazioni complesse

## Rischi
- parsing HTML email non uniforme
- thread grouping imperfetto tra provider
- limiti IMAP/SMTP del provider
- credenziali mail gestite male
- allegati grandi

## Mitigazioni
- import iniziale limitato
- fallback thread by subject + references
- retry sync
- storage separato allegati
- test con una sola casella prima di allargare

## Criteri di accettazione MVP
- login funzionante per due utenti
- casella `info@villamargheritarimini.com` collegata
- email ricevute visibili in inbox
- thread apribile
- risposta inviata correttamente
- note interne persistenti
- assegnazione funzionante
- stato conversazione modificabile
- accesso da due PC diversi

## Piano implementativo concreto
1. Creare schema DB per utenti, mailbox, conversazioni, messaggi e note.
2. Creare backend auth + RBAC.
3. Creare modulo mailbox config con test IMAP/SMTP.
4. Implementare sync iniziale inbox.
5. Implementare inbox UI.
6. Implementare thread UI.
7. Implementare reply/send.
8. Implementare assegnazioni e stati.
9. Testare con te e Veronica.
10. Iterare su filtri, ricerca e template.

## Prompt da mandare a Opus per revisione
```text
Sto progettando una web app interna aziendale per unificare più caselle email in una sola interfaccia, usata da due utenti (admin + operativa), con inbox unificata, thread, reply, assegnazione, note interne e stati.

Ti chiedo di revisionare questa proposta come se fossi un principal engineer:
- valida o critica l'architettura
- evidenzia rischi tecnici e di sicurezza
- proponi uno schema dati migliore se necessario
- suggerisci la stack più adatta per MVP rapido ma solido
- indica cosa togliere dalla V1
- indica cosa aggiungere per non pentirsi dopo

Contesto:
- casella iniziale: info@villamargheritarimini.com
- utenti iniziali: 2
- uso interno da browser
- provider email iniziale tradizionale con IMAP/SMTP
- niente app mobile nella V1

Revisiona il documento allegato e restituisci:
1. architettura consigliata
2. criticità
3. roadmap corretta
4. schema dati corretto
5. backlog MVP ordinato per priorità
```

## Raccomandazione finale
La scelta migliore è:

- non costruire “Spark”
- costruire una vostra app interna email-first
- partire da una sola casella reale
- renderla subito usabile da te e Veronica
- crescere per iterazioni

Questa direzione è fattibile, utile e proporzionata.
