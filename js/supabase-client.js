// js/supabase-client.js

const SUPABASE_URL = "https://tysxeikqbgebpfyblgeb.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InR5c3hlaWtxYmdlYnBmeWJsZ2ViIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUyODczODAsImV4cCI6MjA5MDg2MzM4MH0.yXmS2MFh47eRkCX2voemdBkK5m_tyJiKAFhfyp1YEbw";

window.sb = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true,

    // bypass Web Locks API (fix blocchi 60s)
    lock: async (_name, _acquireTimeout, fn) => await fn(),
  },
});

// session helper
window.getSessionSafe = async function () {
  try {
    const { data, error } = await window.sb.auth.getSession();
    if (error) {
      console.error("Errore getSession:", error);
      return null;
    }
    return data.session ?? null;
  } catch (err) {
    console.error("Errore inatteso getSession:", err);
    return null;
  }
};

// protezione pagine private
window.requireAuth = async function (redirectTo = "/portale.html") {
  const session = await window.getSessionSafe();

  if (!session) {
    window.location.href = redirectTo;
    return null;
  }

  return session;
};

// listener globale auth
window.bindAuthRedirect = function (redirectTo = "/portale.html") {
  window.sb.auth.onAuthStateChange((event, session) => {
    console.log("[AUTH EVENT]", event, session);

    if (!session) {
      window.location.href = redirectTo;
    }
  });
};
