const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const DEFAULT_SUPABASE_ANON_KEY = 'sb_publishable_oMSD-SJgBZAA3Hql6vbxHg_0l2t9S5F';

const INTERNAL_ALLOWED_EMAILS = new Set([
  'fatturazione@illupoaffitta.com',
  'contabilita@illupoaffitta.com',
  'info@marcovenzon.com',
  'veronica.dieta@gmail.com',
  'jessica.appartamenticaldari@gmail.com',
  'cerulliserena@gmail.com',
  'ramirezgonzalezv44@gmail.com',
]);

function isInternalAllowedEmail(email) {
  return INTERNAL_ALLOWED_EMAILS.has(String(email || '').trim().toLowerCase());
}

function normalizeSupabaseUrl(value, fallback = DEFAULT_SUPABASE_URL) {
  const rawValue = String(value || '').trim();
  const fallbackValue = String(fallback || DEFAULT_SUPABASE_URL).trim() || DEFAULT_SUPABASE_URL;

  if (!rawValue) {
    return fallbackValue;
  }

  const sanitizedValue = rawValue.replace(/^["']|["']$/g, '');
  if (!sanitizedValue) {
    return fallbackValue;
  }

  if (/^https?:\/\//i.test(sanitizedValue)) {
    return sanitizedValue.replace(/\/+$/g, '');
  }

  const withoutMaskedPrefix = sanitizedValue.replace(/^[*.]+/, '');
  if (!withoutMaskedPrefix) {
    return fallbackValue;
  }

  const candidate = `https://${withoutMaskedPrefix.replace(/\/+$/g, '')}`;

  try {
    const parsed = new URL(candidate);
    const hostname = String(parsed.hostname || '').toLowerCase();
    if (!hostname.includes('supabase.co') && !hostname.includes('supabase.in')) {
      return fallbackValue;
    }
    return parsed.origin;
  } catch (_error) {
    return fallbackValue;
  }
}

function getSupabaseRuntimeConfig() {
  return {
    url: normalizeSupabaseUrl(process.env.SUPABASE_RUNTIME_URL || process.env.SUPABASE_URL),
    anonKey:
      process.env.SUPABASE_ANON_KEY ||
      process.env.SUPABASE_PUBLISHABLE_KEY ||
      process.env.SUPABASE_ANON_PUBLIC_KEY ||
      DEFAULT_SUPABASE_ANON_KEY,
  };
}

function getBearerToken(headers = {}) {
  const authHeader = headers.authorization || headers.Authorization || '';
  if (!String(authHeader).startsWith('Bearer ')) return '';
  return authHeader.replace('Bearer ', '').trim();
}

function base64UrlEncode(value) {
  return Buffer.from(value)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function base64UrlDecode(value) {
  const normalized = String(value || '')
    .replace(/-/g, '+')
    .replace(/_/g, '/');
  const padding = normalized.length % 4;
  const padded = normalized + (padding ? '='.repeat(4 - padding) : '');
  return Buffer.from(padded, 'base64').toString('utf8');
}

function parsePasswordList(rawValue) {
  return String(rawValue || '')
    .split(/[\n,]/)
    .map((value) => value.trim())
    .filter(Boolean);
}

function getSharedSessionSecret() {
  return String(
    process.env.APP_SESSION_SECRET ||
      process.env.INTERNAL_API_KEY ||
      process.env.INTERNAL_KEY ||
      process.env.SUPABASE_SERVICE_ROLE_KEY ||
      process.env.SUPABASE_SERVICE_KEY ||
      '',
  ).trim();
}

function signSharedSession(payload, options = {}) {
  const secret = getSharedSessionSecret();
  if (!secret) {
    throw new Error('APP_SESSION_SECRET non configurato');
  }

  const now = Math.floor(Date.now() / 1000);
  const ttlSeconds = Number(options.ttlSeconds || 12 * 60 * 60);
  const body = {
    ...payload,
    iat: now,
    exp: now + ttlSeconds,
  };
  const encoded = base64UrlEncode(JSON.stringify(body));
  const signature = crypto
    .createHmac('sha256', secret)
    .update(encoded)
    .digest('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
  return `${encoded}.${signature}`;
}

function verifySharedSession(token) {
  const secret = getSharedSessionSecret();
  if (!secret) return null;
  const [encoded, signature] = String(token || '').split('.');
  if (!encoded || !signature) return null;

  const expected = crypto
    .createHmac('sha256', secret)
    .update(encoded)
    .digest('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');

  const signatureBuffer = Buffer.from(signature);
  const expectedBuffer = Buffer.from(expected);
  if (signatureBuffer.length !== expectedBuffer.length) {
    return null;
  }

  if (!crypto.timingSafeEqual(signatureBuffer, expectedBuffer)) {
    return null;
  }

  let payload;
  try {
    payload = JSON.parse(base64UrlDecode(encoded));
  } catch (_error) {
    return null;
  }

  const now = Math.floor(Date.now() / 1000);
  if (!payload?.exp || payload.exp < now) return null;
  return payload;
}

async function getSupabaseUserFromToken(token) {
  const cleanToken = String(token || '').trim();
  if (!cleanToken) return null;

  const config = getSupabaseRuntimeConfig();
  const client = createClient(config.url, config.anonKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const { data, error } = await client.auth.getUser(cleanToken);
  if (error || !data?.user) return null;
  return data.user;
}

async function getInternalSupabaseUserFromHeaders(headers = {}, allowedEmails = INTERNAL_ALLOWED_EMAILS) {
  const token = getBearerToken(headers);
  if (!token) return null;
  const user = await getSupabaseUserFromToken(token);
  const email = String(user?.email || '').trim().toLowerCase();
  if (!email || !allowedEmails.has(email)) return null;
  return { user, email, token };
}

async function getAuthContextFromHeaders(headers = {}, options = {}) {
  const token = getBearerToken(headers);
  if (!token) return null;

  const sharedSession = verifySharedSession(token);
  if (sharedSession) {
    const allowedSharedRoles = Array.isArray(options.allowedSharedRoles)
      ? options.allowedSharedRoles
      : [];
    if (!allowedSharedRoles.length || allowedSharedRoles.includes(sharedSession.role)) {
      return {
        kind: 'shared',
        token,
        role: sharedSession.role || '',
        app: sharedSession.app || '',
        actor: sharedSession.sub || sharedSession.role || 'shared-app',
        session: sharedSession,
      };
    }
  }

  if (options.allowInternalSupabase !== false) {
    const allowedEmails = options.allowedSupabaseEmails || INTERNAL_ALLOWED_EMAILS;
    const internalUser = await getInternalSupabaseUserFromHeaders(headers, allowedEmails);
    if (internalUser) {
      return {
        kind: 'supabase',
        token,
        role: 'internal',
        app: 'supabase',
        actor: internalUser.email,
        email: internalUser.email,
        user: internalUser.user,
      };
    }
  }

  return null;
}

module.exports = {
  INTERNAL_ALLOWED_EMAILS,
  getAuthContextFromHeaders,
  getBearerToken,
  getInternalSupabaseUserFromHeaders,
  getSharedSessionSecret,
  getSupabaseRuntimeConfig,
  getSupabaseUserFromToken,
  isInternalAllowedEmail,
  normalizeSupabaseUrl,
  parsePasswordList,
  signSharedSession,
  verifySharedSession,
};
