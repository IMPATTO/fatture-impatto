const path = require('path');
const { promisify } = require('util');
const { execFile } = require('child_process');
const { createClient } = require('@supabase/supabase-js');

const execFileAsync = promisify(execFile);
const SCRIPT_PATH = path.resolve(__dirname, '../../scripts/revenue-optimizer.mjs');
const MAX_WINDOW_DAYS = 15;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-internal-key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return respond(204, {});
  }

  if (!['GET', 'POST'].includes(event.httpMethod)) {
    return respond(405, { error: 'Method not allowed' });
  }

  const env = {
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY,
    INTERNAL_API_KEY: process.env.INTERNAL_API_KEY || process.env.INTERNAL_KEY || '',
  };

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    return respond(500, { error: 'Configurazione Supabase mancante' });
  }

  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const auth = await authorizeRequest(event, supabase, env);
  if (!auth.allowed) {
    return respond(auth.statusCode || 401, { error: auth.error || 'Unauthorized' });
  }

  try {
    const request = parseRequest(event);
    const execution = await runOptimizerScript(request);
    const reportTodo = request.createTodo
      ? await createReportTodo(supabase, execution.payload, request, auth.actor)
      : null;

    await safeInsertAuditLog(supabase, {
      user_email: auth.actor,
      action: 'REVENUE_OPTIMIZER_RUN',
      table_name: 'calendar_days',
      record_id: null,
      created_at: new Date().toISOString(),
      new_data: {
        strategy: request.strategy,
        window_days: request.windowDays,
        today: request.today || null,
        create_todo: request.createTodo,
        summary: execution.payload.summary || null,
      },
    });

    return respond(200, {
      success: true,
      request: {
        strategy: request.strategy,
        window_days: request.windowDays,
        today: request.today || null,
        create_todo: request.createTodo,
      },
      summary: execution.payload.summary || null,
      todo: reportTodo,
      markdown: execution.markdown,
      actions: execution.payload.actions || [],
    });
  } catch (error) {
    return respond(500, { error: error.message || 'Revenue optimizer failed' });
  }
};

function parseRequest(event) {
  const query = event.queryStringParameters || {};
  let body = {};
  if (event.body) {
    try {
      body = JSON.parse(event.body);
    } catch (_error) {
      body = {};
    }
  }

  const strategy = String(body.strategy || query.strategy || 'combined').trim() || 'combined';
  const format = String(body.format || query.format || 'json').trim() || 'json';
  const today = normalizeIsoDate(body.today || query.today || '');
  const requestedWindow = Number.parseInt(body.window_days || body.windowDays || query.window_days || query.windowDays || `${MAX_WINDOW_DAYS}`, 10);
  const windowDays = clampWindowDays(requestedWindow);
  const createTodoRaw = body.create_todo ?? body.createTodo ?? query.create_todo ?? query.createTodo;
  const createTodo = createTodoRaw === undefined ? true : parseBoolean(createTodoRaw);

  return {
    strategy,
    format: format === 'markdown' ? 'markdown' : 'json',
    today,
    windowDays,
    createTodo,
  };
}

async function authorizeRequest(event, supabase, env) {
  const hostHeader = String(event.headers.host || event.headers.Host || '').trim().toLowerCase();
  const isLocalDev = Boolean(process.env.NETLIFY_DEV) || hostHeader.startsWith('127.0.0.1:') || hostHeader.startsWith('localhost:');
  if (isLocalDev) {
    return { allowed: true, actor: 'system' };
  }

  const internalKey = String(event.headers['x-internal-key'] || '').trim();
  if (internalKey && internalKey === env.INTERNAL_API_KEY) {
    return { allowed: true, actor: 'system' };
  }

  const authHeader = String(event.headers.authorization || event.headers.Authorization || '').trim();
  if (!authHeader.startsWith('Bearer ')) {
    return { allowed: false, statusCode: 401, error: 'Unauthorized' };
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const { data, error } = await supabase.auth.getUser(token);
  if (error || !data?.user) {
    return { allowed: false, statusCode: 401, error: 'Unauthorized' };
  }

  return { allowed: true, actor: data.user.email || 'user' };
}

async function runOptimizerScript(request) {
  const args = [SCRIPT_PATH, `--strategy=${request.strategy}`, `--window-days=${request.windowDays}`, '--format=json'];
  if (request.today) args.push(`--today=${request.today}`);

  const { stdout, stderr } = await execFileAsync(process.execPath, args, {
    cwd: path.resolve(__dirname, '../..'),
    env: process.env,
    maxBuffer: 1024 * 1024 * 4,
  });

  if (stderr && stderr.trim()) {
    console.warn('[revenue-optimizer] stderr', stderr.trim());
  }

  let payload;
  try {
    payload = JSON.parse(stdout);
  } catch (_error) {
    throw new Error('Output JSON non valido dallo script revenue-optimizer');
  }

  const markdown = renderMarkdownSummary(payload);
  return { payload, markdown };
}

function renderMarkdownSummary(payload) {
  const summary = payload?.summary || {};
  const actions = Array.isArray(payload?.actions) ? payload.actions : [];
  const lines = [
    `Revenue optimizer: ${payload?.strategy || 'combined'}`,
    `Finestra: ${payload?.windowDays || MAX_WINDOW_DAYS} giorni`,
    `Notti aperte: ${summary.openNights ?? 0}`,
    `Alloggi a bassa occupazione: ${summary.lowOccupancyUnits ?? 0}`,
    `Buchi 1 notte: ${summary.oneNightGaps ?? 0}`,
    `Buchi 2 notti: ${summary.twoNightGaps ?? 0}`,
    `Azioni proposte: ${summary.proposedActions ?? actions.length}`,
  ];

  const topActions = actions.slice(0, 8).map((action) =>
    `- ${action.apartmentName} / ${action.unitLabel} / ${action.dateFrom}: ${action.currentPrice ?? 'n.d.'} -> ${action.recommendedPrice ?? 'n.d.'} (${action.tags.join(', ')})`
  );

  if (topActions.length) {
    lines.push('');
    lines.push('Top azioni:');
    lines.push(...topActions);
  }

  return lines.join('\n');
}

async function createReportTodo(supabase, payload, request, actor) {
  const summary = payload?.summary || {};
  const markdown = renderMarkdownSummary(payload);
  const dueDate = payload?.today || normalizeIsoDate(new Date().toISOString().slice(0, 10)) || null;
  const task = [
    `[Revenue] ${request.strategy} - finestra ${request.windowDays} giorni`,
    markdown,
  ].join('\n\n');

  const insertPayload = {
    task,
    priority: request.strategy === 'report' ? 'media' : 'alta',
    due_date: dueDate,
    apartment: null,
    source: 'revenue_optimizer',
    done: false,
  };

  const { data, error } = await supabase
    .from('agent_todos')
    .insert(insertPayload)
    .select('id,task,priority,due_date,source,created_at')
    .single();

  if (error) {
    console.error('[revenue-optimizer] agent_todos insert error', error);
    return {
      error: error.message,
      summary,
      actor,
    };
  }

  return data;
}

async function safeInsertAuditLog(supabase, payload) {
  try {
    const { error } = await supabase.from('audit_log').insert(normalizeAuditLogPayload(payload));
    if (error) {
      console.error('[revenue-optimizer] audit_log error', error);
    }
  } catch (error) {
    console.error('[revenue-optimizer] audit_log fatal', error);
  }
}

function normalizeAuditLogPayload(payload) {
  const normalized = {
    user_email: payload.user_email || '',
    action: payload.action || '',
    table_name: payload.table_name || '',
    record_id: payload.record_id == null ? null : String(payload.record_id),
    old_data: payload.old_data ?? null,
    new_data: payload.new_data ?? payload.payload ?? null,
    ip_address: payload.ip_address ?? null,
  };

  if (payload.created_at) {
    normalized.created_at = payload.created_at;
  }

  return normalized;
}

function parseBoolean(value) {
  if (typeof value === 'boolean') return value;
  const normalized = String(value || '').trim().toLowerCase();
  return ['1', 'true', 'yes', 'y', 'si'].includes(normalized);
}

function clampWindowDays(days) {
  if (!Number.isFinite(days)) return MAX_WINDOW_DAYS;
  return Math.max(1, Math.min(MAX_WINDOW_DAYS, days));
}

function normalizeIsoDate(value) {
  const raw = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : '';
}

function respond(statusCode, body) {
  return {
    statusCode,
    headers: CORS,
    body: statusCode === 204 ? '' : JSON.stringify(body),
  };
}
