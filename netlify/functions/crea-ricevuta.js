// crea-ricevuta.js — Netlify Function per creare ricevute su Fatture in Cloud API v2
// Variabili d'ambiente richieste in Netlify:
//   FATTURE_CLOUD_TOKEN       = token Fatture in Cloud
//   FATTURE_CLOUD_COMPANY_ID  = Company ID (es. 1581288)

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
  if (event.httpMethod !== "POST") {
    return respond(405, { error: "Method not allowed" });
  }

  const token = process.env.FATTURE_CLOUD_TOKEN;
  const companyId = process.env.FATTURE_CLOUD_COMPANY_ID;

  if (!token || !companyId) {
    return respond(500, {
      error: "Variabili d'ambiente FATTURE_CLOUD_TOKEN e FATTURE_CLOUD_COMPANY_ID mancanti",
    });
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch {
    return respond(400, { error: "JSON body non valido" });
  }

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

  let itemName = description || "Soggiorno appartamento";
  if (apartment) itemName += ` — ${apartment}`;
  if (check_in && check_out) itemName += ` (${check_in} → ${check_out})`;

  const docDate = date || new Date().toISOString().split("T")[0];

  const net = Number(net_price);
  const gross = Number((net * (1 + Number(vat_rate) / 100)).toFixed(2));

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
          net_price: net,
          vat: { id: vatId },
          order: 1,
        },
      ],
      payments_list: [
        {
          amount: gross,
          paid_date: docDate,
        },
      ],
      is_marked: false,
      e_invoice: false,
      ...(notes && { notes }),
    },
  };

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

    const doc = data.data;
    return respond(201, {
      success: true,
      document: {
        id: doc.id,
        type: doc.type,
        number: doc.number,
        date: doc.date,
        client_name: doc.entity?.name || doc.client?.name || null,
        net_worth: doc.amount_net,
        gross_worth: doc.amount_gross,
        url: doc.url,
      },
    });
  } catch (err) {
    console.error("Network error:", err);
    return respond(500, {
      error: "Errore di rete verso Fatture in Cloud",
      message: err.message,
    });
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
