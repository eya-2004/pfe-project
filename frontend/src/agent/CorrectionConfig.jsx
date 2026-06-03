import React, { useState, useEffect } from 'react';
import axios from 'axios';
import Icon from '../shared/Icon';
import { useAuth } from './useAuth';
import './CorrectionConfig.css';

const api = axios.create({
  baseURL: '/api',
  withCredentials: true,
});

const CorrectionConfig = () => {
  const { email: userEmail } = useAuth();

  const [availableRuns, setAvailableRuns]           = useState([]);
  const [selectedRunId, setSelectedRunId]           = useState('');
  const [selectedRunInfo, setSelectedRunInfo]       = useState(null);
  const [loadingRuns, setLoadingRuns]               = useState(false);

  const [runErrors, setRunErrors]                   = useState({});
  const [loadingErrors, setLoadingErrors]           = useState(false);

  const [transformationRules, setTransformationRules] = useState({});

  const [bindings, setBindings]                     = useState({});

  const [iterationId, setIterationId]               = useState(null);
  const [lastSavedIterationId, setLastSavedIterationId] = useState(null);

  const [saving, setSaving]                         = useState(false);
  const [launching, setLaunching]                   = useState(false);
  const [alerts, setAlerts]                         = useState([]);
  const [showConfirmModal, setShowConfirmModal]     = useState(false);
  const [confirmAction, setConfirmAction]           = useState(null);
  const [confirmMessage, setConfirmMessage]         = useState('');

  useEffect(() => {
    initializeData();
  }, []);

 
  const initializeData = async () => {
    setLoadingRuns(true);
    try {
      const [runsRes, iterRes, lastRes] = await Promise.allSettled([
        api.get('/correction-config/available-runs'),
        api.get('/correction-config/next-iteration-id'),
        
      ]);

      if (runsRes.status === 'fulfilled') {
        setAvailableRuns(runsRes.value.data || []);
      }
      if (iterRes.status === 'fulfilled') {
        setIterationId(iterRes.value.data);
      }
      
    } catch (e) {
      addAlert('error', 'Erreur lors du chargement initial');
    } finally {
      setLoadingRuns(false);
    }
  };

  const handleSelectRun = async (runId) => {
    setSelectedRunId(runId);
    setBindings({});
    setRunErrors({});
    setTransformationRules({});

    if (!runId) return;

    const run = availableRuns.find(r => r.run_id === runId);
    setSelectedRunInfo(run || null);

    setLoadingErrors(true);
    try {
      const { data } = await api.get(`/correction-config/run-errors/${runId}`);
      
      // Grouper par table puis colonne
      const grouped = {};
      for (const item of (data || [])) {
        if (!grouped[item.tableName]) grouped[item.tableName] = {};
        if (!grouped[item.tableName][item.columnName]) {
          grouped[item.tableName][item.columnName] = [];
        }
        grouped[item.tableName][item.columnName].push({
          ruleLabel:    item.ruleLabel,
          nbViolations: item.nbViolations,
        });
      }
      setRunErrors(grouped);

      // Pour chaque table/colonne, charger les règles de transformation disponibles
      const tables = Object.keys(grouped);
      const transRules = {};
      await Promise.all(tables.map(async (tableName) => {
        const columns = Object.keys(grouped[tableName]);
        transRules[tableName] = {};
        await Promise.all(columns.map(async (columnName) => {
          try {
            const { data: rulesData } = await api.get(
              `/correction-config/transformation-rules`,
              { params: { tableName, columnName } }
            );
            transRules[tableName][columnName] = rulesData || [];
          } catch {
            transRules[tableName][columnName] = [];
          }
        }));
      }));
      setTransformationRules(transRules);

    } catch (e) {
      addAlert('error', 'Erreur lors du chargement des erreurs du run');
    } finally {
      setLoadingErrors(false);
    }
  };

  const handleBindRule = (tableName, columnName, detectionRuleLabel, transformationRule) => {
    const key = `${tableName}::${columnName}::${detectionRuleLabel}`;
    setBindings(prev => ({
      ...prev,
      [key]: {
        tableName,
        columnName,
        detectionRuleLabel,
        transformationRuleId:    transformationRule ? transformationRule.ruleId    : null,
        transformationRuleLabel: transformationRule ? transformationRule.ruleLabel : null,
        transformationRuleType:  transformationRule ? transformationRule.ruleType  : null,
      }
    }));
  };

  const getBinding = (tableName, columnName, detectionRuleLabel) => {
    const key = `${tableName}::${columnName}::${detectionRuleLabel}`;
    return bindings[key] || null;
  };

  // Sélectionner automatiquement la première règle de transformation pour toutes les erreurs
  const handleAutoBindAll = () => {
    const newBindings = { ...bindings };
    for (const [tableName, columns] of Object.entries(runErrors)) {
      for (const [columnName, errors] of Object.entries(columns)) {
        const availableRulesForCol = transformationRules[tableName]?.[columnName] || [];
        if (availableRulesForCol.length === 0) continue;
        for (const error of errors) {
          const key = `${tableName}::${columnName}::${error.ruleLabel}`;
          if (!newBindings[key]) {
            newBindings[key] = {
              tableName,
              columnName,
              detectionRuleLabel:      error.ruleLabel,
              transformationRuleId:    availableRulesForCol[0].ruleId,
              transformationRuleLabel: availableRulesForCol[0].ruleLabel,
              transformationRuleType:  availableRulesForCol[0].ruleType,
            };
          }
        }
      }
    }
    setBindings(newBindings);
  };

  const handleClearBindings = () => setBindings({});



const handleSave = async () => {


  if (!selectedRunId) {
    addAlert('warning', '⚠️ Sélectionnez un run de détection');
    return;
  }

  const boundEntries = Object.values(bindings).filter(b => b.transformationRuleId != null);

  if (boundEntries.length === 0) {
    addAlert('warning', '⚠️ Associez au moins une règle de transformation');
    return;
  }


  const tablesMap = {};
  boundEntries.forEach(b => {
    if (!tablesMap[b.tableName]) {
      tablesMap[b.tableName] = {
        tableName:       b.tableName,
        batchSize:       null,
        toExecute:       true,
        selectedRuleIds: []
      };
    }
    if (!tablesMap[b.tableName].selectedRuleIds.includes(b.transformationRuleId)) {
      tablesMap[b.tableName].selectedRuleIds.push(b.transformationRuleId);
    }
  });

  const payload = {
    iterationId,
    createdBy:      userEmail || 'agent_inconnu',
    detectionRunId: selectedRunId,
    selectedTables: Object.values(tablesMap),
    ruleBindings:   boundEntries.map(b => ({
      tableName:              b.tableName,
      columnName:             b.columnName,
      detectionRuleLabel:     b.detectionRuleLabel,
      transformationRuleId:   b.transformationRuleId,
      transformationRuleType: b.transformationRuleType,
      targetColumn:           b.columnName,
    })),
  };


  try {
    setSaving(true);
    const { data } = await api.post('/correction-config/save', payload);
    setLastSavedIterationId(data.iterationId);
    addAlert('success', ` Itération #${data.iterationId} sauvegardée — ${boundEntries.length} règles`);
    await initializeData();
  } catch (e) {
    addAlert('error', '❌ ' + (e.response?.data?.message || e.message));
  } finally {
    setSaving(false);
  }
};

  const handleLaunch = () => {
    


    openConfirm(
      `🚀 Lancer le DAG ETL pour l'itération #${lastSavedIterationId} ?\n` +
      `📊 Run de détection : ${selectedRunId}\n` +
      `👤 Utilisateur : ${userEmail}`,
      async () => {
        try {
          setLaunching(true);
          const { data } = await api.post('/correction-config/launch-etl', {
          iterationId:   lastSavedIterationId,
          detectionRunId: selectedRunId,
          triggeredBy:   userEmail || 'unknown',
        });
          addAlert('success', `✅ DAG lancé ! Run ID : ${data.dagRunId}`);
        } catch (e) {
          addAlert('error', '❌ ' + (e.response?.data?.message || e.message));
        } finally {
          setLaunching(false);
        }
      }
    );
  };


  const addAlert = (type, message) => {
    const id = Date.now();
    setAlerts(prev => [...prev, { id, type, message }]);
    setTimeout(() => setAlerts(prev => prev.filter(a => a.id !== id)), 6000);
  };

  const openConfirm = (message, onConfirm) => {
    setConfirmMessage(message);
    setConfirmAction(() => onConfirm);
    setShowConfirmModal(true);
  };

  const closeConfirm = () => {
    setShowConfirmModal(false);
    setConfirmAction(null);
  };

  const executeConfirm = () => {
    if (confirmAction) confirmAction();
    closeConfirm();
  };


  const totalErrors = Object.values(runErrors).reduce((acc, cols) =>
    acc + Object.values(cols).reduce((a, errs) => a + errs.length, 0), 0
  );
  const totalBound = Object.values(bindings).filter(b => b.transformationRuleId != null).length;


  return (
    <div className="correction-config-container">


      <div className="config-header">
        <div className="header-title">
          <Icon name="settings" size={28} />
          <h1>Configuration des corrections</h1>
        </div>
        <div className="header-user">
          <Icon name="user" size={18} />
          <span>{userEmail || 'Chargement...'}</span>
        </div>
        <button className="refresh-btn" onClick={initializeData} title="Rafraîchir">
          <Icon name="refresh" size={20} />
        </button>
      </div>

      <div className="config-toolbar">
        <div className="toolbar-info">
          <span className="iteration-id">
            <Icon name="id" size={16} />
            ITÉRATION N° <strong>{iterationId}</strong>
          </span>
          {totalErrors > 0 && (
            <span className="rules-count">
              <Icon name="check-circle" size={16} />
              ERREURS : <strong>{totalErrors}</strong> | ASSOCIÉES : <strong>{totalBound}</strong>
            </span>
          )}
        </div>
        <div className="toolbar-actions">
          {totalErrors > 0 && (
            <>
              <button className="btn btn-ghost" onClick={handleAutoBindAll} title="Associer automatiquement la première règle disponible">
                <Icon name="magic" size={16} /> Auto-associer
              </button>
              <button className="btn btn-ghost" onClick={handleClearBindings}>
                <Icon name="trash" size={16} /> Vider
              </button>
            </>
          )}
          <button
            className="btn btn-secondary"
            onClick={handleSave}
            disabled={saving || totalBound === 0}
          >
            <Icon name="save" size={16} />
            {saving ? 'Sauvegarde...' : 'Sauvegarder'}
          </button>
          <button
            className="btn btn-primary"
            onClick={handleLaunch}
            disabled={launching || !lastSavedIterationId || !selectedRunId}

          >
            <Icon name="rocket" size={16} />
            {lastSavedIterationId && selectedRunId ? `Lancer DAG #${lastSavedIterationId}` : 'Lancer DAG'}
          </button>
        </div>
      </div>

      <div className="cc-section">
        <div className="cc-section-header">
          <span className="cc-step-badge">1</span>
          <h2>Sélectionner un run de détection</h2>
        </div>

        {loadingRuns ? (
          <div className="cc-loading">⏳ Chargement...</div>
        ) : availableRuns.length === 0 ? (
          <div className="cc-empty">⚠️ Aucun run disponible. Lancez d'abord le DAG de détection.</div>
        ) : (
          <div className="cc-select-run-container">
            <label className="cc-select-label">
              <Icon name="search" size={16} />
              Choisir un run :
            </label>
            
            <select
              className="cc-select-run-dropdown"
              value={selectedRunId}
              onChange={(e) => handleSelectRun(e.target.value)}
            >
              <option value="">— Sélectionner un run —</option>
              {availableRuns.map((run, idx) => (
                <option key={`run-${run.run_id}-${idx}`} value={run.run_id}>
                  {run.run_id}
                </option>
              ))}
            </select>
            
           
          </div>
        )}
      </div>

      {selectedRunId && (
        <div className="cc-section">
          <div className="cc-section-header">
            <span className="cc-step-badge">2</span>
            <h2>Associer une règle de transformation à chaque erreur</h2>
            {loadingErrors && <span className="cc-loading-inline">⏳ Chargement...</span>}
          </div>

          {!loadingErrors && totalErrors === 0 && (
            <div className="cc-empty">✅ Aucune erreur trouvée pour ce run.</div>
          )}

          {!loadingErrors && Object.entries(runErrors).map(([tableName, columns]) => (
            <div key={`table-${tableName}`} className="cc-table-block">

              
              <div className="cc-table-header">
                <Icon name="database" size={16} />
                <strong>{tableName}</strong>
                <span className="cc-table-error-count">
                  {Object.values(columns).reduce((a, errs) => a + errs.length, 0)} groupe(s) d'erreurs
                </span>
              </div>

              {Object.entries(columns).map(([columnName, errors], colIndex) => {
                const availableTransRules = transformationRules[tableName]?.[columnName] || [];

                return (
                  <div key={`col-${tableName}-${columnName}-${colIndex}`} className="cc-column-block">

                    {/* En-tête colonne */}
                    <div className="cc-column-header">
                      <Icon name="columns" size={14} />
                      <span className="cc-column-name">{columnName}</span>
                      {availableTransRules.length === 0 && (
                        <span className="cc-badge cc-badge--warn">Aucune règle de transformation disponible</span>
                      )}
                    </div>

                    {/* Une ligne par erreur de détection */}
                    {errors.map((error, errorIndex) => {
                      const binding = getBinding(tableName, columnName, error.ruleLabel);
                      const isBound = binding?.transformationRuleId != null;

                      return (
                        <div
                          key={`error-${tableName}-${columnName}-${error.ruleLabel}-${errorIndex}`}
                          className={`cc-error-row ${isBound ? 'cc-error-row--bound' : 'cc-error-row--unbound'}`}
                        >
                          {/* Colonne gauche : info erreur */}
                          <div className="cc-error-info">
                            <div className="cc-error-label">
                              <span className="cc-error-icon">⚠️</span>
                              <code>{error.ruleLabel}</code>
                            </div>
                            <div className="cc-error-violations">
                              {Number(error.nbViolations || 0).toLocaleString()} ligne(s) en erreur
                            </div>
                          </div>

                         
                          <div className="cc-arrow">→</div>

                          {/* Colonne droite : sélection règle de transformation */}
                          <div className="cc-transformation-select">
                            {availableTransRules.length === 0 ? (
                              <span className="cc-no-rules">— Pas de règle disponible —</span>
                            ) : (
                              <select
                                value={binding?.transformationRuleId ?? ''}
                                onChange={e => {
                                  const ruleId = e.target.value;
                                  if (!ruleId) {
                                    handleBindRule(tableName, columnName, error.ruleLabel, null);
                                  } else {
                                    const rule = availableTransRules.find(
                                      r => String(r.ruleId) === String(ruleId)
                                    );
                                    handleBindRule(tableName, columnName, error.ruleLabel, rule);
                                  }
                                }}
                                className={`cc-select ${isBound ? 'cc-select--bound' : ''}`}
                              >
                                <option value="">— Choisir une transformation —</option>
                                {availableTransRules.map(rule => (
                                  <option key={`rule-${rule.ruleId}`} value={rule.ruleId}>
                                    [{rule.ruleType}] {rule.ruleLabel}
                                    {rule.defaultValue ? ` → "${rule.defaultValue}"` : ''}
                                  </option>
                                ))}
                              </select>
                            )}
                            {isBound && (
                              <span className="cc-bound-badge">✓ Associée</span>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
          ))}
        </div>
      )}

      {/* ── ÉTAPE 3 : Résumé ── */}
      {totalBound > 0 && (
        <div className="cc-section">
          <div className="cc-section-header">
            <span className="cc-step-badge">3</span>
            <h2>Résumé — {totalBound} association(s) prête(s)</h2>
          </div>
          <div className="cc-summary-table">
            <table>
              <thead>
                <tr>
                  <th>Table</th>
                  <th>Colonne</th>
                  <th>Erreur de détection</th>
                  <th>Règle de transformation</th>
                </tr>
              </thead>
              <tbody>
                {Object.values(bindings)
                  .filter(b => b.transformationRuleId != null)
                  .map((b, i) => (
                    <tr key={i}>
                      <td><code>{b.tableName}</code></td>
                      <td><code>{b.columnName}</code></td>
                      <td><span className="cc-badge cc-badge--error">{b.detectionRuleLabel}</span></td>
                      <td>
                        <span className="cc-badge cc-badge--transform">
                          [{b.transformationRuleType}] {b.transformationRuleLabel}
                        </span>
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── ALERTES ── */}
      <div className="cc-alerts-container">
        {alerts.map(alert => (
          <div key={alert.id} className={`cc-alert cc-alert--${alert.type}`}>
            <span>{alert.type === 'success' ? '✓' : alert.type === 'error' ? '✕' : '⚠'}</span>
            <span className="cc-alert-text">{alert.message}</span>
            <button onClick={() => setAlerts(prev => prev.filter(a => a.id !== alert.id))}>✕</button>
          </div>
        ))}
      </div>

      {/* ── MODAL CONFIRMATION ── */}
      {showConfirmModal && (
        <div className="cc-modal-overlay" onClick={closeConfirm}>
          <div className="cc-modal-box" onClick={e => e.stopPropagation()}>
            <div className="cc-modal-icon">🚀</div>
            <h3 className="cc-modal-title">Confirmation</h3>
            <p className="cc-modal-text" style={{ whiteSpace: 'pre-line' }}>{confirmMessage}</p>
            <div className="cc-modal-actions">
              <button className="cc-modal-btn cc-modal-btn--cancel" onClick={closeConfirm}>Annuler</button>
              <button className="cc-modal-btn cc-modal-btn--confirm" onClick={executeConfirm}>Confirmer</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default CorrectionConfig;