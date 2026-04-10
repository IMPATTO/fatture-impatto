// migrate-encrypt-passwords.js
// Script one-shot: cifra tutti i valori in chiaro nella colonna password_encrypted
// di alloggiati_accounts usando AES-256-GCM.
//
// Esegui UNA SOLA VOLTA con:
//   ENCRYPTION_KEY=<tua_chiave> SUPABASE_URL=<url> SUPABASE_SERVICE_ROLE_KEY=<key> node migrate-encrypt-passwords.js
//
// Lo script è idempotente: salta i valori già cifrati (contengono ':').

const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

function encrypt(plaintext) {
  const key = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return iv.toString('hex') + ':' + authTag.toString('hex') + ':' + encrypted.toString('hex');
}

async function main() {
  if (!process.env.ENCRYPTION_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    console.error('❌ Variabili d\'ambiente mancanti: ENCRYPTION_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }

  const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

  // Leggi tutti gli account
  const { data: accounts, error } = await supabase
    .from('alloggiati_accounts')
    .select('id, username, password_encrypted');

  if (error) {
    console.error('❌ Errore lettura account:', error.message);
    process.exit(1);
  }

  console.log(`\nTrovati ${accounts.length} account da verificare...\n`);

  let cifrati = 0, saltati = 0, errori = 0;

  for (const acc of accounts) {
    const pwd = acc.password_encrypted;

    // Già cifrato (formato iv:tag:encrypted)
    if (pwd && pwd.includes(':') && pwd.split(':').length === 3) {
      console.log(`⏭  ${acc.username} — già cifrato, skip`);
      saltati++;
      continue;
    }

    if (!pwd) {
      console.log(`⚠️  ${acc.username} — password vuota, skip`);
      saltati++;
      continue;
    }

    // Cifra
    const encrypted = encrypt(pwd);
    const { error: updateErr } = await supabase
      .from('alloggiati_accounts')
      .update({ password_encrypted: encrypted })
      .eq('id', acc.id);

    if (updateErr) {
      console.error(`❌ ${acc.username} — errore aggiornamento:`, updateErr.message);
      errori++;
    } else {
      console.log(`✅ ${acc.username} — cifrato con successo`);
      cifrati++;
    }
  }

  console.log(`\n────────────────────────────────`);
  console.log(`Cifrati:  ${cifrati}`);
  console.log(`Saltati:  ${saltati}`);
  console.log(`Errori:   ${errori}`);
  console.log(`────────────────────────────────`);

  if (errori === 0 && cifrati > 0) {
    console.log('\n✅ Migrazione completata. Ora deploya send-alloggiati.js aggiornato.');
  } else if (cifrati === 0 && saltati > 0) {
    console.log('\nℹ️  Nessuna migrazione necessaria — tutti già cifrati.');
  }
}

main().catch(err => {
  console.error('Errore fatale:', err);
  process.exit(1);
});
