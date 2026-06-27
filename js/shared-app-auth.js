(function () {
  function storageKey(app) {
    return `shared_app_auth:${String(app || '').trim()}`;
  }

  function readStorage(storage, key) {
    try {
      return storage?.getItem(key) || '';
    } catch (_error) {
      return '';
    }
  }

  function writeStorage(storage, key, value) {
    try {
      storage?.setItem(key, value);
      return true;
    } catch (_error) {
      return false;
    }
  }

  function removeStorage(storage, key) {
    try {
      storage?.removeItem(key);
    } catch (_error) {
      // Ignore storage cleanup failures.
    }
  }

  function readSessionRaw(app) {
    const key = storageKey(app);
    return readStorage(localStorage, key) || readStorage(sessionStorage, key);
  }

  function loadSession(app) {
    const raw = readSessionRaw(app);
    if (!raw) return null;

    try {
      const parsed = JSON.parse(raw);
      if (!parsed?.token) return null;
      if (parsed.expires_at && new Date(parsed.expires_at).getTime() <= Date.now()) {
        clearSession(app);
        return null;
      }
      saveSession(app, parsed);
      return parsed;
    } catch (_error) {
      clearSession(app);
      return null;
    }
  }

  function saveSession(app, session) {
    const payload = {
      app,
      token: session?.token || '',
      role: session?.role || '',
      expires_at: session?.expires_at || null,
    };
    const key = storageKey(app);
    const raw = JSON.stringify(payload);
    writeStorage(localStorage, key, raw);
    writeStorage(sessionStorage, key, raw);
    return payload;
  }

  function clearSession(app) {
    const key = storageKey(app);
    removeStorage(localStorage, key);
    removeStorage(sessionStorage, key);
  }

  function getToken(app) {
    return loadSession(app)?.token || '';
  }

  async function login(app, password) {
    const response = await fetch('/.netlify/functions/shared-app-login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ app, password }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(payload.error || 'Login fallito');
      error.status = response.status;
      error.payload = payload;
      throw error;
    }
    return saveSession(app, payload);
  }

  async function fetchJson(url, options = {}) {
    const app = options.app || '';
    const token = options.token || (app ? getToken(app) : '');
    const headers = {
      ...(options.headers || {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    };

    const response = await fetch(url, {
      method: options.method || 'GET',
      headers,
      body: options.body,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(payload.error || `HTTP ${response.status}`);
      error.status = response.status;
      error.payload = payload;
      throw error;
    }
    return payload;
  }

  window.sharedAppAuth = {
    clearSession,
    fetchJson,
    getToken,
    loadSession,
    login,
    saveSession,
  };
})();
