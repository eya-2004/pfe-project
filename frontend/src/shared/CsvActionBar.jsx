// src/components/CsvActionBar.jsx
import React, { useState, useRef } from 'react';
import axios from 'axios';

const api = axios.create({ withCredentials: true });

const Toast = ({ toast }) => {
  if (!toast) return null;
  const cfg = {
    success: { bg: '#d1fae5', border: '#a7f3d0', color: '#065f46', icon: '✓' },
    error:   { bg: '#fee2e2', border: '#fecaca', color: '#b91c1c', icon: '✕' },
    warning: { bg: '#fef3c7', border: '#fde68a', color: '#d97706', icon: '!' },
  }[toast.type] || {};
  return (
    <div style={{
      position: 'fixed', bottom: 20, right: 20, zIndex: 9999,
      background: cfg.bg,
      border: `1px solid ${cfg.border}`,
      borderRadius: 8, padding: '12px 16px',
      display: 'flex', alignItems: 'center', gap: 12,
      boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
      maxWidth: 350,
    }}>
      <span style={{
        width: 24, height: 24, borderRadius: '50%',
        background: cfg.bg, border: `1px solid ${cfg.border}`,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        color: cfg.color, fontWeight: 'bold', fontSize: 14,
      }}>{cfg.icon}</span>
      <span style={{ color: cfg.color, fontSize: 14 }}>{toast.message}</span>
    </div>
  );
};

export default function CsvActionBar({ tableName }) {
  const [exporting, setExporting] = useState(false);
  const [importing, setImporting] = useState(false);
  const [toast, setToast] = useState(null);
  const fileInputRef = useRef(null);

  const showToast = (type, message) => {
    setToast({ type, message });
    setTimeout(() => setToast(null), 3000);
  };

  const handleExport = async () => {
    if (!tableName) { 
      showToast('warning', 'Veuillez sélectionner une table à exporter.'); 
      return; 
    }
    
    setExporting(true);
    try {
      const response = await api.get(`/api/export/csv?status=PENDING&table=${tableName}`, { responseType: 'blob' });
      
      if (response.status === 204) {
        showToast('warning', `Aucune ligne PENDING à exporter pour « ${tableName} ».`); 
        return;
      }
      
      const date = new Date().toISOString().slice(0, 10);
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const a = Object.assign(document.createElement('a'), { 
        href: url, 
        download: `${tableName.toLowerCase()}_erreurs_${date}.csv` 
      });
      document.body.appendChild(a); 
      a.click(); 
      a.remove();
      window.URL.revokeObjectURL(url);
      showToast('success', `Export de « ${tableName} » téléchargé avec succès.`);
    } catch { 
      showToast('error', "Erreur lors de l'export. Vérifiez la connexion."); 
    }
    finally { 
      setExporting(false); 
    }
  };

  const handleFileSelected = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    e.target.value = '';
    
    if (!file.name.endsWith('.csv')) { 
      showToast('error', 'Le fichier doit être au format .csv'); 
      return; 
    }
    
    setImporting(true);
    try {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('correctedBy', 'agent_migration');
      formData.append('comment', `Import du ${new Date().toLocaleString('fr-FR')}`);
      
      const { data } = await api.post('/api/export/csv/import', formData, { 
        headers: { 'Content-Type': 'multipart/form-data' } 
      });
      
      if (data.success) {
        const n = data.updated;
        showToast('success', `Import réussi — ${n} ligne${n > 1 ? 's' : ''} corrigée${n > 1 ? 's' : ''}.`);
      } else { 
        showToast('error', data.message || "L'import a échoué."); 
      }
    } catch (err) {
      showToast('error', err.response?.data?.message || "Erreur lors de l'import.");
    } finally { 
      setImporting(false); 
    }
  };

  return (
    <>
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        gap: 12,
        background: '#f9fafb',
        border: '1px solid #e5e7eb',
        borderRadius: 8,
        padding: '12px 16px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 16 }}>📁</span>
          <div>
            <p style={{ fontSize: 14, fontWeight: 'bold', margin: 0 }}>Fichiers CSV</p>
            <p style={{ fontSize: 12, color: '#6b7280', margin: 0 }}>Exporter les erreurs PENDING · Importer les corrections</p>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8, marginLeft: 'auto' }}>
          <button
            onClick={handleExport}
            disabled={exporting || importing}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              padding: '8px 12px',
              borderRadius: 6,
              border: 'none',
              background: exporting || importing ? '#d1d5db' : '#3b82f6',
              color: 'white',
              fontSize: '14px',
              cursor: exporting || importing ? 'not-allowed' : 'pointer',
            }}
          >
            {exporting ? (
              <>Exportation...</>
            ) : (
              <>↓ Exporter CSV</>
            )}
          </button>

          <button
            onClick={() => fileInputRef.current?.click()}
            disabled={importing || exporting}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              padding: '8px 12px',
              borderRadius: 6,
              border: 'none',
              background: importing || exporting ? '#d1d5db' : '#10b981',
              color: 'white',
              fontSize: '14px',
              cursor: importing || exporting ? 'not-allowed' : 'pointer',
            }}
          >
            {importing ? (
              <>Importation...</>
            ) : (
              <>↑ Importer CSV</>
            )}
          </button>

          <input
            ref={fileInputRef}
            type="file"
            accept=".csv"
            style={{ display: 'none' }}
            onChange={handleFileSelected}
          />
        </div>
      </div>
      <Toast toast={toast} />
    </>
  );
}