import React, { useState, useEffect } from 'react';
import axios from 'axios';
import Icon from '../shared/Icon';
import { useAuth } from './useAuth';  // ✅ IMPORTER LE MÊME HOOK
import './CorrectionConfig.css';

const api = axios.create({
  baseURL: '/api',
  withCredentials: true,
});

const CorrectionConfig = () => {
  // ✅ Récupérer l'email DYNAMIQUEMENT (comme dans IterationConfig)
  const { email: userEmail } = useAuth();

  const [tables, setTables] = useState([]);
  const [selectedTables, setSelectedTables] = useState(new Set());
  const [tableConfigs, setTableConfigs] = useState(new Map());
  const [iterationId, setIterationId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
const [lastSavedIterationId, setLastSavedIterationId] = useState(null); 
// ─── MODALS & ALERTES ──────────────────────────────
const [showConfirmModal, setShowConfirmModal] = useState(false);
const [confirmAction, setConfirmAction] = useState(null);
const [confirmMessage, setConfirmMessage] = useState('');

const [alerts, setAlerts] = useState([]); // Tableau d'alertes
  useEffect(() => {
    initializeData();
    
    // ✅ DEBUG : Vérifier que l'email est bien récupéré
    console.log('✅ Email utilisateur (CorrectionConfig):', userEmail);
  }, []);

  const initializeData = async () => {
  try {
    setLoading(true);

    const [tablesResult, iterationResult, lastIdResult] = await Promise.allSettled([
      api.get('/correction-config/tables'),
      api.get('/correction-config/next-iteration-id'),
      api.get('/correction-config/last-saved-iteration-id')
    ]);

    if (tablesResult.status === 'fulfilled') {
      setTables(tablesResult.value.data);
      const initialConfigs = new Map();
      tablesResult.value.data.forEach(table => {
        initialConfigs.set(table.tableName, {
          batchSize: '',
          selectedRules: new Set()
        });
      });
      setTableConfigs(initialConfigs);
    }

    if (iterationResult.status === 'fulfilled') {
      setIterationId(iterationResult.value.data);
    }

    if (lastIdResult.status === 'fulfilled' && lastIdResult.value.data != null) {
      setLastSavedIterationId(lastIdResult.value.data);
    }

  } catch (error) {
    console.error('Erreur lors du chargement:', error);
    alert('Erreur lors du chargement');
  } finally {
    setLoading(false);
  }
};
    // ─── FONCTIONS MODALS & ALERTES ────────────────────

    const openConfirm = (message, onConfirm) => {
      setConfirmMessage(message);
      setConfirmAction(() => onConfirm);
      setShowConfirmModal(true);
    };

    const closeConfirm = () => {
      setShowConfirmModal(false);
      setConfirmAction(null);
      setConfirmMessage('');
    };

    const executeConfirm = () => {
      if (confirmAction) confirmAction();
      closeConfirm();
    };

    // Ajouter une alerte
    const addAlert = (type, message) => {
      const id = Date.now();
      setAlerts(prev => [...prev, { id, type, message }]);
      // Auto-suppression après 5 secondes
      setTimeout(() => {
        setAlerts(prev => prev.filter(a => a.id !== id));
      }, 5000);
    };

    // Supprimer une alerte manuellement
    const removeAlert = (id) => {
      setAlerts(prev => prev.filter(a => a.id !== id));
    };
  const handleSelectAllTables = (checked) => {
    if (checked) {
      const allTableNames = new Set(tables.map(t => t.tableName));
      setSelectedTables(allTableNames);
    } else {
      setSelectedTables(new Set());
      const resetConfigs = new Map();
      tables.forEach(table => {
        resetConfigs.set(table.tableName, { batchSize: '', selectedRules: new Set() });
      });
      setTableConfigs(resetConfigs);
    }
  };

  const handleToggleTable = (tableName) => {
    const newSelected = new Set(selectedTables);
    const newConfigs = new Map(tableConfigs);
    
    if (newSelected.has(tableName)) {
      newSelected.delete(tableName);
      newConfigs.set(tableName, { batchSize: '', selectedRules: new Set() });
    } else {
      newSelected.add(tableName);
    }
    
    setSelectedTables(newSelected);
    setTableConfigs(newConfigs);
  };

  const handleBatchSizeChange = (tableName, value) => {
    const newConfigs = new Map(tableConfigs);
    const currentConfig = newConfigs.get(tableName) || { batchSize: '', selectedRules: new Set() };
    currentConfig.batchSize = value === '' ? '' : parseInt(value) || '';
    newConfigs.set(tableName, currentConfig);
    setTableConfigs(newConfigs);
  };

  const handleToggleRule = (tableName, ruleId) => {
    const newConfigs = new Map(tableConfigs);
    const currentConfig = newConfigs.get(tableName) || { batchSize: '', selectedRules: new Set() };
    const newSelectedRules = new Set(currentConfig.selectedRules);
    
    if (newSelectedRules.has(ruleId)) {
      newSelectedRules.delete(ruleId);
    } else {
      newSelectedRules.add(ruleId);
    }
    
    currentConfig.selectedRules = newSelectedRules;
    newConfigs.set(tableName, currentConfig);
    setTableConfigs(newConfigs);
  };

  const handleSelectAllRulesForTable = (tableName, rules) => {
    const newConfigs = new Map(tableConfigs);
    const currentConfig = newConfigs.get(tableName) || { batchSize: '', selectedRules: new Set() };
    const allRuleIds = rules.map(r => r.ruleId);
    const currentSelected = currentConfig.selectedRules;
    
    const allSelected = allRuleIds.every(id => currentSelected.has(id));
    const newSelectedRules = allSelected ? new Set() : new Set(allRuleIds);
    
    currentConfig.selectedRules = newSelectedRules;
    newConfigs.set(tableName, currentConfig);
    setTableConfigs(newConfigs);
  };


const handleLaunchAirflow = async () => {
  if (!lastSavedIterationId) {
    addAlert('error', '❌ Aucune itération sauvegardée à lancer.');
    return;
  }

  openConfirm(
    `🚀 Lancer Airflow pour l'Itération #${lastSavedIterationId} ?\n👤 Utilisateur: ${userEmail}`,
    async () => {
      try {
        const response = await api.post(`/correction-config/launch-airflow/${lastSavedIterationId}`, {
          triggeredBy: userEmail || 'unknown'
        });

        const { dagRunId, status } = response.data;
        addAlert('success', `✅ DAG lancé ! Itération #${lastSavedIterationId} | DAG: ${dagRunId}`);
      } catch (error) {
        console.error('Erreur lancement Airflow:', error);
        addAlert('error', '❌ Erreur lancement Airflow: ' + (error.response?.data?.message || error.message));
      }
    }
  );
};

// ── handleSave (remplacer les alert) ──
const handleSave = async () => {
  if (selectedTables.size === 0) {
    addAlert('warning', '⚠️ Veuillez sélectionner au moins une table');
    return;
  }
  const tablesWithoutRules = Array.from(selectedTables).filter(tableName => {
    const config = tableConfigs.get(tableName);
    return !config || config.selectedRules.size === 0;
  });

  if (tablesWithoutRules.length > 0) {
    addAlert(
      'warning',
      `⚠️ Veuillez sélectionner au moins une règle pour : ${tablesWithoutRules.join(', ')}`
    );
    return;
  }
  try {
    setSaving(true);
    
    const payload = {
      iterationId,
      createdBy: userEmail || 'agent_inconnu',
      selectedTables: Array.from(selectedTables).map(tableName => {
        const config = tableConfigs.get(tableName) || { batchSize: '', selectedRules: new Set() };
        return {
          tableName,
          batchSize: config.batchSize === '' ? null : config.batchSize,
          toExecute: true,
          selectedRuleIds: Array.from(config.selectedRules)
        };
      })
    };

    console.log('Payload envoyé:', payload);
    const response = await api.post('/correction-config/save', payload);
    
    const savedId = response.data.iterationId;
    setLastSavedIterationId(savedId);
    
    addAlert('success', `✅ Itération #${savedId} sauvegardée ! Tables: ${response.data.totalTablesSaved}, Règles: ${response.data.totalRulesSaved}`);
    
    await initializeData();
    
  } catch (error) {
    console.error('Erreur:', error);
    addAlert('error', '❌ Erreur: ' + (error.response?.data?.message || error.message));
  } finally {
    setSaving(false);
  }
};
const totalSelectedRules = Array.from(selectedTables).reduce((total, tableName) => {
  const config = tableConfigs.get(tableName);
  return total + (config?.selectedRules?.size || 0);
}, 0);

const allTablesSelected = tables.length > 0 && selectedTables.size === tables.length;
  return (
    <div className="correction-config-container">
      {/* Header */}
      <div className="config-header">
        <div className="header-title">
          <Icon name="settings" size={28} />
          <h1>Configuration des itérations de correction</h1>
        </div>
        <div className="header-user">
          <Icon name="user" size={18} />
          
          <span>{userEmail || 'Chargement...'}</span>
        </div>
        <button className="refresh-btn" onClick={initializeData} title="Rafraîchir">
          <Icon name="refresh" size={20} />
        </button>
      </div>

      {/* Toolbar */}
      <div className="config-toolbar">
        <div className="toolbar-info">
          <span className="iteration-id">
            <Icon name="id" size={16} />
            ITÉRATION N° <strong>{iterationId}</strong>
          </span>
          <span className="auto-badge">
            <Icon name="lock" size={14} /> Auto-Incrément
          </span>
          <span className="rules-count">
            <Icon name="check-circle" size={16} />
            TABLE(S): <strong>{selectedTables.size}</strong> | 
            RÈGLE(S): <strong>{totalSelectedRules}</strong>
          </span>
        </div>
        <div className="toolbar-actions">
          
          <button 
            className="btn btn-secondary" 
            onClick={handleSave}
            disabled={saving || selectedTables.size === 0}
          >
            <Icon name="save" size={16} />
              {saving ? 'Sauvegarde...' : `Sauvegarder itération`}
          </button>

          <button className="btn btn-primary" onClick={handleLaunchAirflow} disabled={!lastSavedIterationId}>
            <Icon name="rocket" size={16} />
            {lastSavedIterationId ? `Lancer DAG #${lastSavedIterationId}` : 'Lancer Airflow'} 
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="config-content-fullwidth">
        <div className="tables-panel-full">
          <label className="select-all-checkbox">
            <input
              type="checkbox"
              checked={allTablesSelected}
              onChange={(e) => handleSelectAllTables(e.target.checked)}
            />
            <Icon name="database" size={16} />
            Toutes les tables ({tables.length})
          </label>

          <div className="tables-header">
            <Icon name="folder" size={18} />
            <h3>Tables disponibles</h3>
          </div>

          <div className="tables-list">
            {tables.map((table) => {
              const isSelected = selectedTables.has(table.tableName);
              const config = tableConfigs.get(table.tableName) || { 
                batchSize: '', 
                selectedRules: new Set() 
              };
              const allRulesSelected = table.rules && table.rules.length > 0 &&
                table.rules.every(r => config.selectedRules.has(r.ruleId));

              return (
                <div 
                  key={table.tableName} 
                  className={`table-item ${isSelected ? 'selected' : ''}`}
                >
                  <div className="table-info">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => handleToggleTable(table.tableName)}
                    />
                    <div className="table-details">
                      <div className="table-name">
                        <Icon name="file-text" size={14} />
                        {table.tableName}
                      </div>
                      
                      <div className="table-rows">
                        📊 <strong>{table.totalRows?.toLocaleString()}</strong> lignes
                      </div>
                      
                      {isSelected && (
                        <div className="batch-size-input">
                          <label>
                            <Icon name="layers" size={14} />
                            Taille du batch (lignes):
                          </label>
                          <input
                            type="number"
                            value={config.batchSize}
                            onChange={(e) => handleBatchSizeChange(table.tableName, e.target.value)}
                            min="1"
                            max={table.totalRows || 999999}
                            placeholder="Ex: 5000"
                          />
                          <span className="batch-hint">Laissez vide pour tout traiter</span>
                        </div>
                      )}
                    </div>
                  </div>
                  
                  {isSelected && table.rules && table.rules.length > 0 && (
                    <div className="rules-section">
                      <div className="rules-header">
                        <input
                          type="checkbox"
                          checked={allRulesSelected}
                          onChange={() => handleSelectAllRulesForTable(table.tableName, table.rules)}
                        />
                        <Icon name="check-square" size={16} />
                        <span>Toutes les règles ({table.rules.length})</span>
                      </div>
                      
                      <div className="rules-list">
                        {table.rules.map((rule) => (
                          <div key={rule.ruleId} className="rule-item">
                            <input
                              type="checkbox"
                              checked={config.selectedRules.has(rule.ruleId)}
                              onChange={() => handleToggleRule(table.tableName, rule.ruleId)}
                            />
                            <div className="rule-info">
                              <div className="rule-column-name">
                                <strong>{rule.sourceColumn}</strong>
                                {rule.ruleLabel && (
                                  <span className="rule-label">- {rule.ruleLabel}</span>
                                )}
                              </div>
                              <div className="rule-meta">
                                <span className="rule-type">{rule.ruleType}</span>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>
            {/* ── ALERTES MODERNES ── */}
      <div className="cc-alerts-container">
        {alerts.map(alert => (
          <div key={alert.id} className={`cc-alert cc-alert--${alert.type}`}>
            <span className="cc-alert-icon">
              {alert.type === 'success' && '✓'}
              {alert.type === 'error' && '✕'}
              {alert.type === 'warning' && '⚠'}
            </span>
            <span className="cc-alert-text">{alert.message}</span>
            <button 
              className="cc-alert-close" 
              onClick={() => removeAlert(alert.id)}
            >
              ✕
            </button>
          </div>
        ))}
      </div>

      {/* ── MODAL DE CONFIRMATION ── */}
      {showConfirmModal && (
        <div className="cc-modal-overlay" onClick={closeConfirm}>
          <div className="cc-modal-box" onClick={e => e.stopPropagation()}>
            
            <div className="cc-modal-icon">⚠</div>
            
            <h3 className="cc-modal-title">Confirmation</h3>
            
            <p className="cc-modal-text">{confirmMessage}</p>
            
            <div className="cc-modal-actions">
              <button 
                className="cc-modal-btn cc-modal-btn--cancel" 
                onClick={closeConfirm}
              >
                Annuler
              </button>
              <button 
                className="cc-modal-btn cc-modal-btn--confirm" 
                onClick={executeConfirm}
              >
                Confirmer
              </button>
            </div>
            
          </div>
        </div>
      )}
    </div>
  );
};

export default CorrectionConfig;