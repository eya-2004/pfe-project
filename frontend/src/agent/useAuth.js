// hooks/useAuth.js
import { useState, useEffect } from 'react';

export function useAuth() {
  const [user, setUser] = useState(null);

  useEffect(() => {
    //Récupère l'email stocké lors du Login
    const email = sessionStorage.getItem("pendingMail");
    
    if (email) {
      setUser({ email });
    }
  }, []);

  // Helper pour nettoyer
  const logout = () => {
    sessionStorage.removeItem("pendingMail");
    setUser(null);
  };

  return { 
    user, 
    email: user?.email || null,
    isAuthenticated: !!user 
  };
}