import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";

// Fonction de validation du format email
const isValidEmail = (email) => {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
};

export default function Login() {
  const [mail, setMail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError("");

    if (!isValidEmail(mail)) {
      setError("Format email incorrect");
      setLoading(false);
      return;
    }

    if (!password || password.trim() === "") {
      setError("Mot de passe requis");
      setLoading(false);
      return;
    }

    try {
      const formData = new FormData();
      formData.append("mail", mail);
      formData.append("password", password);

      const response = await axios.post("/api/auth/login", formData, {
        withCredentials: true,
        headers: { "Content-Type": "multipart/form-data" },
      });

      const { role, mustChangePassword, mail: userMail } = response.data;
      console.log("response.data:", response.data);
      console.log("userMail:", userMail);
      
      sessionStorage.setItem("pendingMail", userMail);
      if (role === "ROLE_ADMIN") {
          navigate("/admin/dashboard");
      } else if (role === "ROLE_AGENT_MIGRATION") {
          if (mustChangePassword) {
              navigate("/change-password");  
          } else {
              navigate("/agent/dashboard");  
          }
      } else {
          setError("Rôle non reconnu: " + role);
      }
    } catch (err) {
      if (err.code === "ERR_NETWORK") {
        setError("Impossible de contacter le serveur.");
      } else if (err.response) {
        const data = err.response.data;
        const status = err.response.status;
        
        if (status === 401) {
          // Distinction entre les différentes erreurs d'authentification
          if (data?.error?.toLowerCase().includes("email") || 
              data?.error?.toLowerCase().includes("mail") ||
              data?.error?.toLowerCase().includes("utilisateur") ||
              data?.error?.toLowerCase().includes("user")) {
            setError("Utilisateur non trouvé");
          } else if (data?.error?.toLowerCase().includes("password") || 
                     data?.error?.toLowerCase().includes("mot de passe")) {
            setError("Mot de passe incorrect");
          } else {
            setError(data?.error || "Email ou mot de passe incorrect.");
          }
        } else if (status === 400) {
          // Erreurs de validation
          if (typeof data === "string") {
            setError(data);
          } else if (data?.message) {
            setError(data.message);
          } else {
           ("Erreur de validation des données");
          }
        } else {
          setError(
            typeof data === "string"
              ? data
              : data?.message || data?.error || "Erreur de connexion"
          );
        }
      } else {
        setError("Erreur de connexion au serveur");
      }
    } finally {
      setLoading(false);
    }
  };

  const handleEmailChange = (e) => {
    setMail(e.target.value);
  };
  const handlePasswordChange = (e) => {
    setPassword(e.target.value);
};
 

  return (
    <div className="auth-page">
      <div className="auth-card">
        <h2 className="auth-title">Connexion</h2>
        <p className="auth-subtitle">Système de validation des données</p>

        {error && <div className="auth-alert error">{error}</div>}

        <form onSubmit={handleSubmit}>
          <div className="auth-field">
            <label className="auth-label">Email</label>
            <input
              type="text"
              value={mail}
              onChange={handleEmailChange}
              placeholder="Entrez votre email"
              className="auth-input"
              
            />
            
          </div>
          
          <div className="auth-field">
            <label className="auth-label">Mot de passe</label>
            <input
              type="password"
              value={password}
              onChange={handlePasswordChange}
              placeholder="Entrez votre mot de passe"
              className="auth-input"
             
            />
          </div>
          
          <button type="submit" className="auth-button" disabled={loading}>
            {loading ? "Connexion..." : "Se connecter"}
          </button>
        </form>
      </div>
    </div>
  );
}