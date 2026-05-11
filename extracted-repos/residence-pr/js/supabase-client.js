const SUPABASE_URL = 'https://tysxeikqbgebpfyblgeb.supabase.co';
const SUPABASE_ANON_KEY = 'sb_publishable_oMSD-SJgBZAA3Hql6vbxHg_0l2t9S5F';

window.sb = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true,
    lock: async (_name, _acquireTimeout, fn) => await fn(),
  },
});
