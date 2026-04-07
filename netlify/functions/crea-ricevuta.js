// crea-ricevuta.js — Netlify Function per creare ricevute su Fatture in Cloud API v2
// Variabili d'ambiente richieste in Netlify:
//   FIC_API_TOKEN    = Bearer token Fatture in Cloud
//   FIC_COMPANY_ID   = Company ID (es. 1581288)

const FIC_BASE = "https://api-v2.fattureincloud.it";

// VAT IDs del tuo account FiC (da GET /info/vat_types)
const VAT_IDS = {
  22: 0,
  10: 3,
  4: 4,
  5: 54,
  0: 6, // esente
};

const headers = (token) => ({
  Authorization: `Bearer ${token}`,
  Accept: "application/json",
  "Content-Type": "application/json",
});

// ---------- HANDLER ----------

exports.handler = async (event) => {
  // Solo POST
  if (event.httpMethod !== "POST") {
    return respond(405, { error: "Method not allowed" });
  }

  const token = process.env.FATTURE_CLOUD_TOKEN;
const companyId = process.env.FATTURE_CLOUD_COMPANY_ID;

  if (!token || !companyId) {
    return respond(500, { error: "Variabili d'ambiente FATTURE_CLOUD_TOKEN e FATTURE_CLOUD_COMPANY_ID mancanti" });
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch {
    return respond(400, { error: "JSON body non valido" });
  }

  // ---------- Parametri attesi ----------
  // {
  //   client_name:    "Mario Rossi"           (obbligatorio)
  //   client_cf:      "RSSMRA80A01H501Z"      (opzionale)
  //   client_email:   "mario@email.com"        (opzionale)
  //   description:    "Soggiorno appartamento" (opzionale, default fornito)
  //   net_price:      100                      (obbligatorio)
  //   vat_rate:       22                       (opzionale, default 22)
  //   date:           "2026-04-07"             (opzionale, default oggi)
  //   apartment:      "Bilocale Vista Mare"    (opzionale, aggiunto alla descrizione)
  //   check_in:       "2026-04-01"             (opzionale)
  //   check_out:      "2026-04-07"             (opzionale)
  //   notes:          "Nota interna"           (opzionale)
  // }

  const {
    client_name,
    client_cf,
    client_email,
    description,
    net_price,
    vat_rate = 22,
    date,
    apartment,
    check_in,
    check_out,
    notes,
  } = body;

  // Validazione
  if (!client_name || typeof client_name !== "string") {
    return respond(400, { error: "client_name è obbligatorio" });
  }
  if (net_price == null || isNaN(Number(net_price)) || Number(net_price) <= 0) {
    return respond(400, { error: "net_price deve essere un numero > 0" });
  }

  const vatId = VAT_IDS[vat_rate];
  if (vatId === undefined) {
    return respond(400, {
      error: `vat_rate ${vat_rate} non supportato. Valori ammessi: ${Object.keys(VAT_IDS).join(", ")}`,
    });
  }

  // Costruisci descrizione riga
  let itemName = description || "Soggiorno appartamento";
  if (apartment) itemName += ` — ${apartment}`;
  if (check_in && check_out) itemName += ` (${check_in} → ${check_out})`;

  // Data documento
  const docDate = date || new Date().toISOString().split("T")[0];

  // ---------- Payload FiC ----------
  const payload = {
    data: {
      type: "receipt",
      date: docDate,
      currency: { id: "EUR" },
      language: { code: "it", name: "Italiano" },
      entity: buildClient(client_name, client_cf, client_email),
      items_list: [
        {
          name: itemName,
          qty: 1,
          net_price: Number(net_price),
          vat: { id: vatId },
          order: 1,
        },
      ],
      is_marked: false,
      e_invoice: false,
      ...(notes && { notes }),
    },
  };

  // ---------- Chiamata API ----------
  try {
    const url = `${FIC_BASE}/c/${companyId}/issued_documents`;
    const res = await fetch(url, {
      method: "POST",
      headers: headers(token),
      body: JSON.stringify(payload),
    });

    const data = await res.json();

    if (!res.ok) {
      console.error("FiC error:", JSON.stringify(data));
      return respond(res.status, {
        error: "Errore Fatture in Cloud",
        details: data,
      });
    }

    // Risposta con i dati essenziali del documento creato
    const doc = data.data;
    return respond(201, {
      success: true,
      document: {
        id: doc.id,
        type: doc.type,
        number: doc.number,
        date: doc.date,
        client_name: doc.client?.name,
        net_worth: doc.amount_net,
        gross_worth: doc.amount_gross,
        url: doc.url,
      },
    });
  } catch (err) {
    console.error("Network error:", err);
    return respond(500, { error: "Errore di rete verso Fatture in Cloud", message: err.message });
  }
};

// ---------- HELPERS ----------

function buildClient(name, cf, email) {
  const client = {
    name,
    type: "person",
  };
  if (cf) client.tax_code = cf;
  if (email) client.email = email;
  return client;
}

function respond(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    },
    body: JSON.stringify(body),
  };
}
