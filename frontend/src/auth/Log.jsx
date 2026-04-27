import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";

export default function Login() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError("");
    try {
      const formData = new FormData();
      formData.append("Email", email);
      formData.append("Password", password);

      const response = await axios.post("/api/auth/login", formData, {
        withCredentials: true,
        headers: { "Content-Type": "multipart/form-data" }
      });

      const role = response.data?.role;
      if (role === "ROLE_ADMINISTRATEUR") {
        navigate("/admin/dashboard");
      } else if (role === "ROLE_AGENT_MIGRATION") {
        navigate("/agent/dashboard");
      } else if (role) {
        setError("Aucun utilisateur avec ce rôle.");
      } else {
        setError("Réponse de connexion invalide.");
      }
    } catch (err) {
      if (err.code === "ERR_NETWORK") {
        setError("Impossible de contacter le serveur. Vérifiez que le backend tourne sur http://localhost:8082");
      } else if (err.response) {
        setError(err.response.data || "Email ou mot de passe incorrect.");
      } else {
        setError("Erreur de connexion au serveur");
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: "20px" }}>
      <h2>Page de connexion</h2>
      <form onSubmit={handleSubmit}>
        <div>
          <label htmlFor="email">Email:</label>
          <input
            id="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            type="email"
            placeholder="Tapez votre email"
            required
          />
        </div>
        <div>
          <label htmlFor="password">Mot de passe:</label>
          <input
            id="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            type="password"
            placeholder="Tapez votre mot de passe"
          />
        </div>
        <button type="submit" disabled={loading}>
          {loading ? "Connexion..." : "Se connecter"}
        </button>
        {error && <p style={{ color: "red", marginTop: "8px" }}>{error}</p>}
      </form>
    </div>
  );
}
