import { create } from "zustand";

interface User {
  id: string;
  email: string;
  name: string;
  workspaceRoot?: string | null;
  dbPath?: string | null;
}

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  login: (token: string, user: User) => void;
  logout: () => void;
  restoreSession: () => void;
}

export const useAuthStore = create<AuthState>((set) => ({
  user: null,
  token: null,
  isAuthenticated: false,

  login: (token: string, user: User) => {
    if (typeof window !== "undefined") {
      localStorage.setItem("dataforge_token", token);
      localStorage.setItem("dataforge_user", JSON.stringify(user));
      document.cookie = `df_user_id=${encodeURIComponent(user.id)}; path=/; max-age=604800; samesite=lax`;
    }
    set({ token, user, isAuthenticated: true });
  },

  logout: () => {
    if (typeof window !== "undefined") {
      localStorage.removeItem("dataforge_token");
      localStorage.removeItem("dataforge_user");
      document.cookie = "df_user_id=; path=/; max-age=0; samesite=lax";
    }
    set({ token: null, user: null, isAuthenticated: false });
  },

  restoreSession: () => {
    if (typeof window !== "undefined") {
      const token = localStorage.getItem("dataforge_token");
      const userStr = localStorage.getItem("dataforge_user");
      if (token && userStr) {
        try {
          const user = JSON.parse(userStr) as User;
          document.cookie = `df_user_id=${encodeURIComponent(user.id)}; path=/; max-age=604800; samesite=lax`;
          set({ token, user, isAuthenticated: true });
        } catch {
          // Invalid user data, clear storage
          localStorage.removeItem("dataforge_token");
          localStorage.removeItem("dataforge_user");
        }
      }
    }
  },
}));
