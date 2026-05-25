import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const NETLIFY_API_BASE_URL = 'https://api.netlify.com';
const DEFAULT_SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const NETLIFY_CONFIG_PATH = path.join(os.homedir(), 'Library', 'Preferences', 'netlify', 'config.json');
const NETLIFY_STATE_RELATIVE_PATH = path.join('.netlify', 'state.json');
const NETLIFY_ENV_CACHE_RELATIVE_PATH = path.join('.netlify', 'runtime-env-cache.json');

const siteEnvCache = new Map();

export function getEnvValue(name) {
  return String(process.env[name] || '').trim();
}

function readJsonFile(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, 'utf8'));
  } catch (_error) {
    return null;
  }
}

function resolveProjectRoot(projectRoot = process.cwd()) {
  return path.resolve(projectRoot);
}

function readNetlifyAuthToken() {
  const config = readJsonFile(NETLIFY_CONFIG_PATH);
  if (!config?.userId || !config?.users) return '';
  return String(config.users[config.userId]?.auth?.token || '').trim();
}

function readNetlifySiteId(projectRoot = process.cwd()) {
  const statePath = path.join(resolveProjectRoot(projectRoot), NETLIFY_STATE_RELATIVE_PATH);
  const state = readJsonFile(statePath);
  return String(state?.siteId || '').trim();
}

function getCachePath(projectRoot = process.cwd()) {
  return path.join(resolveProjectRoot(projectRoot), NETLIFY_ENV_CACHE_RELATIVE_PATH);
}

function readCachedSiteEnv(projectRoot = process.cwd(), context = 'production') {
  const cache = readJsonFile(getCachePath(projectRoot));
  const items = cache?.contexts?.[context];
  return Array.isArray(items) ? items : [];
}

function writeCachedSiteEnv(projectRoot = process.cwd(), context = 'production', siteId, items) {
  if (!Array.isArray(items)) return;
  const cachePath = getCachePath(projectRoot);
  const existing = readJsonFile(cachePath) || {};
  const next = {
    siteId: siteId || existing.siteId || '',
    updatedAt: new Date().toISOString(),
    contexts: {
      ...(existing.contexts || {}),
      [context]: items,
    },
  };
  mkdirSync(path.dirname(cachePath), { recursive: true });
  writeFileSync(cachePath, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
}

function fetchSiteEnvFromNetlify(projectRoot = process.cwd(), context = 'production') {
  const root = resolveProjectRoot(projectRoot);
  const cacheKey = `${root}:${context}`;
  if (siteEnvCache.has(cacheKey)) return siteEnvCache.get(cacheKey);

  const token = readNetlifyAuthToken();
  const siteId = readNetlifySiteId(root);
  if (!token || !siteId) {
    const cachedItems = readCachedSiteEnv(root, context);
    siteEnvCache.set(cacheKey, cachedItems);
    return cachedItems;
  }

  const url = `${NETLIFY_API_BASE_URL}/api/v1/sites/${siteId}/env?context_name=${encodeURIComponent(context)}`;

  try {
    const stdout = execFileSync('curl', [
      '--silent',
      '--show-error',
      '--location',
      '--header',
      `Authorization: Bearer ${token}`,
      '--header',
      'Accept: application/json',
      url,
    ], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      maxBuffer: 1024 * 1024 * 10,
    });

    const items = JSON.parse(String(stdout || '[]'));
    if (Array.isArray(items)) {
      writeCachedSiteEnv(root, context, siteId, items);
      siteEnvCache.set(cacheKey, items);
      return items;
    }
  } catch (_error) {
    // Fall back to the latest cached copy when Netlify is unreachable in the sandbox.
  }

  const cachedItems = readCachedSiteEnv(root, context);
  siteEnvCache.set(cacheKey, cachedItems);
  return cachedItems;
}

function getContextCandidates(context = 'production') {
  const normalized = String(context || 'production').trim().toLowerCase() || 'production';
  if (normalized === 'production') return ['production', 'all', 'dev'];
  if (normalized === 'dev' || normalized === 'dev-server') return [normalized, 'all', 'production'];
  return [normalized, 'all', 'production', 'dev'];
}

function pickEnvValue(values, context = 'production') {
  if (!Array.isArray(values)) return '';
  for (const candidate of getContextCandidates(context)) {
    const match = values.find((item) => item?.context === candidate || item?.context_parameter === candidate);
    const value = String(match?.value || '').trim();
    if (value) return value;
  }
  return '';
}

export function readNetlifyEnv(name, { projectRoot = process.cwd(), context = 'production' } = {}) {
  const items = fetchSiteEnvFromNetlify(projectRoot, context);
  const match = items.find((item) => item?.key === name);
  return pickEnvValue(match?.values, context);
}

export function resolveRuntimeConfig({
  projectRoot = process.cwd(),
  context = 'production',
  defaultSupabaseUrl = DEFAULT_SUPABASE_URL,
} = {}) {
  const supabaseUrl = getEnvValue('SUPABASE_URL')
    || getEnvValue('SUPABASE_RUNTIME_URL')
    || readNetlifyEnv('SUPABASE_URL', { projectRoot, context })
    || readNetlifyEnv('SUPABASE_RUNTIME_URL', { projectRoot, context })
    || defaultSupabaseUrl;

  const supabaseKey = getEnvValue('SUPABASE_SERVICE_ROLE_KEY')
    || getEnvValue('SUPABASE_SERVICE_KEY')
    || readNetlifyEnv('SUPABASE_SERVICE_ROLE_KEY', { projectRoot, context })
    || readNetlifyEnv('SUPABASE_SERVICE_KEY', { projectRoot, context });

  const beds24RefreshToken = getEnvValue('BEDS24_REFRESH_TOKEN')
    || getEnvValue('BEDS24_API_KEY')
    || readNetlifyEnv('BEDS24_REFRESH_TOKEN', { projectRoot, context })
    || readNetlifyEnv('BEDS24_API_KEY', { projectRoot, context });

  return { supabaseUrl, supabaseKey, beds24RefreshToken };
}
