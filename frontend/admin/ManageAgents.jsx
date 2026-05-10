import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './manageAgents.css';

axios.defaults.withCredentials = true;

export default function ManageAgents() {
  const [agents,      setAgents]      = useState([]);
  const [loading,     setLoading]     = useState(false);
  const [submitted,   setSubmitted]   = useState(false);
  const [error,       setError]       = useState('');
  const [succes,      setSucces]      = useState('');
  const [form,        setForm]        = useState({ firstname: '', lastname: '', mail: '' });
  const [formErrors,  setFormErrors]  = useState({});

  useEffect(() => { fetchAgents(); }, []);

  const fetchAgents = async () => {
    setLoading(true);
    try {
      const { data } = await axios.get('/admin/agents');
      setAgents(data);
    } catch {
      setError('Erreur lors du chargement des agents');
    } finally {
      setLoading(false);
    }
  };

  const validateForm = () => {
    const e = {};
    if (!form.firstname.trim()) e.firstname = 'Le nom est requis';
    if (!form.lastname.trim())  e.lastname  = 'Le prénom est requis';
    if (!form.mail.trim())      e.mail      = "L'email est requis";
    else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(form.mail)) e.mail = "L'email n'est pas valide";
    return e;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(''); setSucces('');
    const errors = validateForm();
    if (Object.keys(errors).length > 0) { setFormErrors(errors); return; }
    setFormErrors({});
    setSubmitted(true);
    try {
      await axios.post(
        `/admin/createAgent?firstname=${encodeURIComponent(form.firstname)}&lastname=${encodeURIComponent(form.lastname)}&mail=${encodeURIComponent(form.mail)}`,
        {}
      );
      setSucces(`Agent créé avec succès ! Email envoyé à ${form.mail}`);
      setForm({ firstname: '', lastname: '', mail: '' });
      fetchAgents();
    } catch (err) {
      setError(err.response?.data || "Erreur lors de la création de l'agent");
    } finally {
      setSubmitted(false);
    }
  };
// Ajouter ces 2 nouveaux states (après les autres states existants)
const [showDeleteModal, setShowDeleteModal] = useState(false);
const [agentToDelete, setAgentToDelete] = useState(null);

// Modifier la fonction handleDelete comme ceci :
const handleDelete = async (id) => {
  setAgentToDelete(id);          // Stocke l'ID
  setShowDeleteModal(true);      // Ouvre le modal
};

// Ajouter cette nouvelle fonction après handleDelete :
const confirmDelete = async () => {
  setShowDeleteModal(false);
  try {
    await axios.delete(`/admin/deleteagent/${agentToDelete}`);
    setSucces('Agent supprimé avec succès');
    fetchAgents();
  } catch {
    setError('Erreur de suppression');
  }
  setAgentToDelete(null);
};

const cancelDelete = () => {
  setShowDeleteModal(false);
  setAgentToDelete(null);
};
  return (
    <div className="ma-page">

      {/* ── En-tête ── */}
      <div className="ma-page-header">
        <div>
          <h1 className="ma-page-title">Gestion des agents</h1>
          <p className="ma-page-sub">Créez et administrez les comptes agents de migration</p>
        </div>
        <div className="ma-agent-count">
          <span className="ma-count-num">{agents.length}</span>
          <span className="ma-count-label">agent{agents.length !== 1 ? 's' : ''}</span>
        </div>
      </div>

      {/* ── Alertes ── */}
      {error  && (
        <div className="ma-alert ma-alert--error">
          <span className="ma-alert-icon">⚠</span>
          <span>{error}</span>
          <button className="ma-alert-close" onClick={() => setError('')}>✕</button>
        </div>
      )}
      {succes && (
        <div className="ma-alert ma-alert--success">
          <span className="ma-alert-icon">✓</span>
          <span>{succes}</span>
          <button className="ma-alert-close" onClick={() => setSucces('')}>✕</button>
        </div>
      )}

      {/* ── Layout deux colonnes ── */}
      <div className="ma-layout">

        {/* ── Formulaire ── */}
        <div className="ma-card ma-form-card">
          <div className="ma-card-header">
            <span className="ma-card-icon">＋</span>
            <h2 className="ma-card-title">Nouvel agent</h2>
          </div>

          <form onSubmit={handleSubmit} className="ma-form">
            <div className="ma-field">
              <label className="ma-label" htmlFor="nom">Nom</label>
              <input
                id="nom"
                type="text"
                placeholder="Dupont"
                className={`ma-input${formErrors.firstname ? ' ma-input--err' : ''}`}
                value={form.firstname}
                onChange={e => setForm({ ...form, firstname: e.target.value })}
              />
              {formErrors.firstname && <span className="ma-field-err">{formErrors.firstname}</span>}
            </div>

            <div className="ma-field">
              <label className="ma-label" htmlFor="prenom">Prénom</label>
              <input
                id="prenom"
                type="text"
                placeholder="Marie"
                className={`ma-input${formErrors.lastname ? ' ma-input--err' : ''}`}
                value={form.lastname}
                onChange={e => setForm({ ...form, lastname: e.target.value })}
              />
              {formErrors.lastname && <span className="ma-field-err">{formErrors.lastname}</span>}
            </div>

            <div className="ma-field">
              <label className="ma-label" htmlFor="email">Email</label>
              <input
                id="email"
                type="email"
                placeholder="marie.dupont@exemple.com"
                className={`ma-input${formErrors.mail ? ' ma-input--err' : ''}`}
                value={form.mail}
                onChange={e => setForm({ ...form, mail: e.target.value })}
              />
              {formErrors.mail && <span className="ma-field-err">{formErrors.mail}</span>}
            </div>

            <button
              type="submit"
              className={`ma-btn-submit${submitted ? ' ma-btn-submit--loading' : ''}`}
              disabled={submitted}
            >
              {submitted ? (
                <>
                  <span className="ma-spinner" />
                  Création en cours…
                </>
              ) : (
                <>
                  <span className="ma-btn-icon">＋</span>
                  Créer l'agent
                </>
              )}
            </button>
          </form>

          {/* Info bulle -->  */}
          <div className="ma-info-tip">
            <span className="ma-tip-icon">✉</span>
            Un email contenant les identifiants sera automatiquement envoyé à l'agent.
          </div>
        </div>

        {/* ── Tableau ── */}
        <div className="ma-card ma-table-card">
          <div className="ma-card-header">
            <span className="ma-card-icon">👥</span>
            <h2 className="ma-card-title">Liste des agents</h2>
          </div>

          {loading ? (
            <div className="ma-table-loading">
              <div className="ma-spinner ma-spinner--lg" />
              <p>Chargement…</p>
            </div>
          ) : agents.length === 0 ? (
            <div className="ma-empty">
              <span className="ma-empty-icon">📭</span>
              <p>Aucun agent .</p>
            </div>
          ) : (
            <div className="ma-table-wrap">
              <table className="ma-table">
                <thead>
                  <tr>
                    <th>Nom</th>
                    <th>Prénom</th>
                    <th>Email</th>
                    <th>Statut</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {agents.map(agent => (
                    <tr key={agent.id}>
                      <td><span className="ma-cell-name">{agent.firstname}</span></td>
                      <td>{agent.lastname}</td>
                      <td><span className="ma-cell-email">{agent.mail}</span></td>
                      <td>
                        {agent.mustChangePassword
                          ? <span className="ma-badge ma-badge--warn">MDP à changer</span>
                          : <span className="ma-badge ma-badge--ok">Actif</span>}
                      </td>
                      <td>
                        <button
                          className="ma-btn-delete"
                          onClick={() => handleDelete(agent.id)}
                          title="Supprimer l'agent"
                        >
                          Supprimer
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
      {/* ── Modal Confirmation Suppression ── */}
      {showDeleteModal && (
        <div className="ma-modal-overlay" onClick={cancelDelete}>
          <div className="ma-modal-box" onClick={e => e.stopPropagation()}>
            
            <div className="ma-modal-icon">⚠</div>
            
            <h3 className="ma-modal-title">Confirmation</h3>
            
            <p className="ma-modal-text">Supprimer cet agent ?</p>
            
            <div className="ma-modal-actions">
              <button 
                className="ma-modal-btn ma-modal-btn--cancel" 
                onClick={cancelDelete}
              >
                Annuler
              </button>
              <button 
                className="ma-modal-btn ma-modal-btn--confirm" 
                onClick={confirmDelete}
              >
                Supprimer
              </button>
            </div>
            
          </div>
        </div>
      )}
    </div>
  );
}