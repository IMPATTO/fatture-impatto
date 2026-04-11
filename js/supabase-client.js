const SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InR5c3hlaWtxYmdlYnBmeWJsZ2ViIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUyODczODAsImV4cCI6MjA5MDg2MzM4MH0.yXmS2MFh47eRkCX2voemdBkK5m_tyJiKAFhfyp1YEbw';

window.sb = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true,
    lock: async (_name, _acquireTimeout, fn) => await fn(),
  },
});
