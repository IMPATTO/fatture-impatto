// netlify/functions/ocr.js
// Riceve immagine base64 + tipo (privato/azienda)
// Chiama Claude API lato server (chiave sicura)
// Restituisce JSON con i dati estratti

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method not allowed' };
  }

  const CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS, body: '' };
  }

  try {
    const { image, tipo } = JSON.parse(event.body);

    if (!image || !tipo) {
      return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing image or tipo' }) };
    }

    const prompt = tipo === 'privato'
      ? `Sei un assistente per l'estrazione dati da documenti italiani e stranieri. Analizza questa immagine di un documento d'identità (carta d'identità, passaporto, patente o tessera sanitaria) e restituisci SOLO un JSON con questi campi:
{"nome":"","cognome":"","codice_fiscale":"","data_nascita":"YYYY-MM-DD","sesso":"M o F","luogo_nascita":"","cittadinanza":"","tipo_documento":"IDENT o PASOR o PATEN","numero_documento":"","luogo_rilascio":"","indirizzo":"","cap":"","citta":"","provincia":"","paese":"Italia"}
Note importanti:
- sesso: usa solo "M" o "F"
- data_nascita: formato YYYY-MM-DD
- tipo_documento: usa IDENT per carta d'identita, PASOR per passaporto, PATEN per patente
- cittadinanza: scrivi il nome del paese in italiano (es. "Italia", "Germania", "Francia")
- luogo_nascita: scrivi solo il nome della citta o comune
- luogo_rilascio: ente o comune che ha rilasciato il documento
- numero_documento: il numero identificativo del documento
Se un campo non è leggibile lascialo stringa vuota. Rispondi SOLO con il JSON, niente altro.`
      : `Sei un assistente per l'estrazione dati da documenti aziendali italiani. Analizza questa immagine (visura camerale, documento aziendale, carta intestata) e restituisci SOLO un JSON:
{"ragione_sociale":"","partita_iva":"","codice_fiscale":"","indirizzo":"","cap":"","citta":"","provincia":"","paese":"Italia","sdi":"","pec":""}
Se un campo non è leggibile lascialo stringa vuota. Rispondi SOLO con il JSON, niente altro.`;

    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 500,
        messages: [{
          role: 'user',
          content: [
            { type: 'image', source: { type: 'base64', media_type: 'image/jpeg', data: image } },
            { type: 'text', text: prompt }
          ]
        }]
      })
    });

    const data = await res.json();
    const text = data.content?.[0]?.text || '{}';
    const clean = text.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(clean);

    return {
      statusCode: 200,
      headers: { ...CORS, 'Content-Type': 'application/json' },
      body: JSON.stringify(parsed)
    };

  } catch (err) {
    console.error('OCR error:', err);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: 'OCR failed' })
    };
  }
};
