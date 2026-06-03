// src/components/ChangePassword.jsx
import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import SideBar   from "../shared/SideBar";



const PwdInput = ({ id, label, value, onChange, placeholder }) => {
  const [show, setShow] = useState(false);
  return (
    <div style={{ marginBottom: 16 }}>
      <label htmlFor={id} style={{ display: 'block', fontSize: 14, fontWeight: 500, marginBottom: 6 }}>{label}</label>
      <div style={{ position: 'relative' }}>
        <input
          id={id} type={show ? 'text' : 'password'}
          value={value} onChange={onChange} placeholder={placeholder}
          style={{
            width: '100%', padding: '10px 40px 10px 12px',
            border: '1px solid #d1d5db', borderRadius: 6,
            fontSize: 14,
          }}
        />
        <button 
          type="button" 
          onClick={() => setShow(s => !s)} 
          style={{
            position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
            background: 'none', border: 'none', cursor: 'pointer', fontSize: 16,
          }}
        >
          {show ? '🙈' : '👁'}
        </button>
      </div>
    </div>
  );
};

export default function ChangePassword() {
  const [oldPassword, setOldPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const mail = sessionStorage.getItem('pendingMail');
  if (!mail) { navigate('/login'); return null; }

  const handleSubmit = async (e) => {
    e.preventDefault(); setError('');
    if (newPassword !== confirm) { setError("Les mots de passe ne correspondent pas."); return; }
    if (newPassword.length < 8)  { setError("Le mot de passe doit contenir au moins 8 caractères."); return; }
    setLoading(true);
    try {
      await axios.post('/agent/change-password', { mail, oldPassword, newPassword }, { withCredentials: true });
      sessionStorage.removeItem('pendingMail');
      setSuccess("Mot de passe enregistré avec succès");
      setTimeout(() => navigate('/login'), 2000);
    } catch (err) {
      setError(err.response?.data || "Erreur lors du changement de mot de passe.");
    } finally { setLoading(false); }
  };

  return (
    <div className="dashboard-shell">
      <SideBar role="AGENT" />
      
      <div className="dashboard-main" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
        <div style={{ width: '100%', maxWidth: '400px' }}>
          <div style={{ background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 4px 6px rgba(0,0,0,0.1)' }}>
            <div style={{ textAlign: 'center', marginBottom: 24 }}>
              <div style={{ fontSize: 32, marginBottom: 8 }}>🔒</div>
              <h1 style={{ fontSize: '20px', fontWeight: 'bold', margin: '0 0 8px' }}>Définir votre mot de passe</h1>
              <p style={{ fontSize: '14px', color: '#6b7280', margin: 0 }}>Sécurisez votre compte en définissant un mot de passe personnel.</p>
            </div>

            <div style={{ padding: '12px', background: '#eff6ff', borderRadius: '6px', marginBottom: 16, textAlign: 'center' }}>
              <span style={{ fontSize: '14px', color: '#1d4ed8', fontWeight: 500 }}>📧 {mail}</span>
            </div>

            {error && (
              <div style={{ padding: '12px', background: '#fee2e2', borderRadius: '6px', color: '#b91c1c', marginBottom: 16, fontSize: '14px' }}>
                {error}
              </div>
            )}

            <form onSubmit={handleSubmit}>
              <PwdInput
                id="old" label="Mot de passe temporaire"
                value={oldPassword} onChange={e => setOldPassword(e.target.value)}
                placeholder="Mot de passe reçu par email"
              />
              
              <div>
                <PwdInput
                    id="new" label="Nouveau mot de passe"
                    value={newPassword} onChange={e => setNewPassword(e.target.value)}
                    placeholder="Minimum 8 caractères"
                />

              </div>
              
              <PwdInput
                id="confirm" label="Confirmer le mot de passe"
                value={confirm} onChange={e => setConfirm(e.target.value)}
                placeholder="Répétez le nouveau mot de passe"
              />
              
              {confirm && newPassword && (
                <p style={{ marginTop: 6, fontSize: 12, fontWeight: 500, color: newPassword === confirm ? '#059669' : '#dc2626' }}>
                  {newPassword === confirm ? '✓ Les mots de passe correspondent' : '✗ Les mots de passe ne correspondent pas'}
                </p>
              )}

              <button
                type="submit"
                disabled={loading || !oldPassword || !newPassword || !confirm}
                style={{
                  width: '100%', padding: '12px', borderRadius: '6px', border: 'none',
                  background: '#3b82f6', color: '#fff', fontSize: '14px', fontWeight: '600',
                  cursor: loading || !oldPassword || !newPassword || !confirm ? 'not-allowed' : 'pointer',
                  opacity: loading || !oldPassword || !newPassword || !confirm ? 0.7 : 1,
                  marginTop: 8,
                }}
              >
                {loading ? 'Enregistrement...' : '🔐 Confirmer le mot de passe'}
              </button>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
}