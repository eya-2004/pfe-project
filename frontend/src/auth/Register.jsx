import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import "./auth.css";

export default function Register() {
  const [form, setForm] = useState({
    firstname: "",
    lastname: "",
    mail: "",
    password: "",
  });
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const navigate = useNavigate();

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    setSuccess("");

    try {
      await axios.post(
        `/api/auth/registerAdmin`,
        null,
        {
          params: {
            firstname: form.firstname,
            lastname: form.lastname,
            mail: form.mail,
            password: form.password,
          },
        }
      );
      setSuccess("Compte créé ! Redirection...");
      setTimeout(() => navigate("/login"), 1500);
    } catch (err) {
      setError(err.response?.data || "Erreur lors de la création.");
    }
  };

  return (
    <div className="auth-page">
      <div className="auth-card">
        <h2 className="auth-title">Créer un compte Admin</h2>

        {error && <div className="auth-alert error">{error}</div>}
        {success && <div className="auth-alert success">{success}</div>}

        <form onSubmit={handleSubmit}>
          {[
            { name: "firstname", label: "Prénom", type: "text" },
            { name: "lastname", label: "Nom", type: "text" },
            { name: "mail", label: "Email", type: "email" },
            { name: "password", label: "Mot de passe", type: "password" },
          ].map((field) => (
            <div key={field.name} className="auth-field">
              <label className="auth-label">{field.label}</label>
              <input
                type={field.type}
                name={field.name}
                value={form[field.name]}
                onChange={handleChange}
                className="auth-input"
                required
              />
            </div>
          ))}

          <button type="submit" className="auth-button">
            Créer le compte
          </button>
        </form>

        <p className="auth-footer">
          Déjà un compte ?{" "}
          <a href="/login" className="auth-link">
            Se connecter
          </a>
        </p>
      </div>
    </div>
  );
}
