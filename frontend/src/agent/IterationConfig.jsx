import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import SideBar from "../shared/SideBar";
import { useAuth } from "./useAuth";
import "./iterationConfig.css";

const api = axios.create({
  baseURL: '/api',
  withCredentials: true,
});

export default function IterationConfig() {
  const { email: userEmail } = useAuth();

  // ── État Mode A ──────────────────────────────────────────────
  const [sourceTables,        setSourceTables]        = useState([]);
  const [selectedSourceTable, setSelectedSourceTable] = useState(null);
  const [sourceTableRules,    setSourceTableRules]    = useState([]);
  const [loadingRules,        setLoadingRules]        = useState(false);
  const [newIterationId,      setNewIterationId]      = useState("");
  const [tableBatchSizes,     setTableBatchSizes]     = useState({});

  // Plan des règles sélectionnées
  const [planModeA, setPlanModeA] = useState({});

  // ── UI état ──────────────────────────────────────────────────
  const [loading,    setLoading]    = useState(false);
  const [error,      setError]      = useState(null);
  const [exporting,  setExporting]  = useState(false);
  const [triggering, setTriggering] = useState(false);
  const [successMsg, setSuccessMsg] = useState(null);
  const [airflowMsg, setAirflowMsg] = useState(null);
  const [dagRunId,   setDagRunId]   = useState(null);

  // ─── Init : Charger tables + ID auto ─────────────────────────
  useEffect(() => {
    loadSourceTables();
    loadNextIterationId();
  }, []);

  // ══════════════════════════════════════════════════════════════
  // CHARGEMENT
  // ══════════════════════════════════════════════════════════════

  const loadSourceTables = async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await api.get("/iterations/available-tables");
      if (data.success) {
        setSourceTables(data.tables || []);
      } else {
        setError(data.error || "Impossible de charger les tables");
      }
    } catch (e) {
      setError("Erreur: " + e.message);
    } finally {
      setLoading(false);
    }
  };

  const loadNextIterationId = async () => {
    try {
      const { data } = await api.get("/iterations/next-iteration-id");
      setNewIterationId(data.toString());
    } catch (e) {
      setNewIterationId("1");
    }
  };

  const loadRulesForTable = async (tableName) => {
    setSelectedSourceTable(tableName);
    setLoadingRules(true);
    try {
      const { data } = await api.get(`/iterations/available-tables/${tableName}/rules`);
      if (data.success) {
        setSourceTableRules(data.columns || []);
      } else {
        setSourceTableRules([]);
      }
    } catch (e) {
      setSourceTableRules([]);
    } finally {
      setLoadingRules(false);
    }
  };

  // ══════════════════════════════════════════════════════════════
  // GESTION DES RÈGLES
  // ══════════════════════════════════════════════════════════════

  const toggleRuleModeA = (tableName, columnName, ruleLabel, ruleId, ruleType) => {
    const key = `${tableName}::${columnName}::${ruleLabel}`;
    setPlanModeA(prev => {
      const next = { ...prev };
      if (next[key]) {
        delete next[key];
      } else {
        next[key] = { 
          tableName, 
          columnName, 
          ruleLabel,
          ruleId,
          ruleType: ruleType || 'SAME'
        };
      }
      return next;
    });
  };

  const isRuleInPlanA = (tableName, columnName, ruleLabel) =>
    !!planModeA[`${tableName}::${columnName}::${ruleLabel}`];

  const setBatchSize = (tableName, value) => {
    setTableBatchSizes(prev => ({ ...prev, [tableName]: value }));
  };

  // ── Sélection globale ────────────────────────────────────────

  const selectAllTablesModeA = async () => {
    const newPlan = { ...planModeA };
    for (const table of sourceTables) {
      try {
        const { data } = await api.get(`/iterations/available-tables/${table.tableName}/rules`);
        if (data.success && data.columns) {
          for (const col of data.columns) {
            for (const rule of (col.rules || [])) {
              const key = `${table.tableName}::${col.columnName}::${rule.ruleLabel}`;
              if (!newPlan[key]) {
                newPlan[key] = {
                  tableName: table.tableName,
                  columnName: col.columnName,
                  ruleLabel: rule.ruleLabel,
                  ruleId: rule.id,
                  ruleType: rule.ruleType || 'SAME'
                };
              }
            }
          }
        }
      } catch (e) {
        console.error(`Erreur pour ${table.tableName}:`, e);
      }
    }
    setPlanModeA(newPlan);
  };

  const deselectAllTablesModeA = () => setPlanModeA({});

  const selectAllRulesModeA = () => {
    if (!selectedSourceTable) return;
    const newPlan = { ...planModeA };
    for (const col of sourceTableRules) {
      for (const rule of (col.rules || [])) {
        const key = `${selectedSourceTable}::${col.columnName}::${rule.ruleLabel}`;
        if (!newPlan[key]) {
          newPlan[key] = {
            tableName: selectedSourceTable,
            columnName: col.columnName,
            ruleLabel: rule.ruleLabel,
            ruleId: rule.id,
            ruleType: rule.ruleType || 'SAME'
          };
        }
      }
    }
    setPlanModeA(newPlan);
  };

  const deselectAllRulesModeA = () => {
    if (!selectedSourceTable) return;
    const newPlan = { ...planModeA };
    const keysToRemove = Object.keys(newPlan).filter(k => newPlan[k].tableName === selectedSourceTable);
    keysToRemove.forEach(key => delete newPlan[key]);
    setPlanModeA(newPlan);
  };

  // ── Calculs état checkboxes ──────────────────────────────────

  const areAllTablesSelectedModeA = () => {
    if (sourceTables.length === 0) return false;
    const tablesWithRules = new Set(Object.values(planModeA).map(p => p.tableName));
    return tablesWithRules.size === sourceTables.length;
  };

  const areAllRulesSelectedForCurrentTable = () => {
    if (!selectedSourceTable || sourceTableRules.length === 0) return false;
    let total = 0, inPlan = 0;
    for (const col of sourceTableRules) {
      const rules = col.rules || [];
      total += rules.length;
      for (const rule of rules) {
        if (planModeA[`${selectedSourceTable}::${col.columnName}::${rule.ruleLabel}`]) inPlan++;
      }
    }
    return total > 0 && inPlan === total;
  };

  const hasSomeRulesForCurrentTable = () => {
    if (!selectedSourceTable) return false;
    return Object.values(planModeA).some(p => p.tableName === selectedSourceTable);
  };

  const hasSomeRulesInAnyTable = () => Object.keys(planModeA).length > 0;

  // ══════════════════════════════════════════════════════════════
  // SAUVEGARDE
  // ══════════════════════════════════════════════════════════════

  const handleSavePlanModeA = async () => {
    const planEntries = Object.values(planModeA);
    if (planEntries.length === 0) return alert("Veuillez sélectionner au moins une règle");
    if (!newIterationId || isNaN(parseInt(newIterationId)))
      return alert("ID d'itération invalide");

    const targetId = parseInt(newIterationId);
    setExporting(true);
    setSuccessMsg(null);
    
    try {
      const itemsToCorrect = planEntries.map(entry => ({
        tableName:    entry.tableName,
        columnName:   entry.columnName,
        rule:         entry.ruleLabel,
        ruleId:       entry.ruleId || null,
        ruleOrigin:   entry.ruleType || 'SAME',
        flagToCheck:  true,
        batchSize:    parseInt(tableBatchSizes[entry.tableName] || 0),
      }));

      const agentEmail = userEmail || "agent_inconnu";
      const response = await api.post("/iterations/correction-plan", {
        sourceIterationId:  0,
        targetIterationId: targetId,
        selectedBy: agentEmail,
        itemsToCorrect,
      });

      if (response.data.success) {
        const sameCount = response.data.sameRules || 0;
        const diffCount = response.data.diffRules || 0;
        
        setSuccessMsg(
          `✅ ${response.data.correctionCount} règle(s) configurées pour l'itération ${targetId}` +
          ` (SAME=${sameCount}, DIFF=${diffCount})`
        );
        setTimeout(() => setSuccessMsg(null), 6000);
      } else {
        alert("Erreur: " + (response.data.error || "Inconnue"));
      }
    } catch (e) {
      alert("Erreur: " + (e.response?.data?.error || e.message));
    } finally {
      setExporting(false);
    }
  };

  const handleTriggerAirflow = async () => {
    const iterIdToLaunch = parseInt(newIterationId);
    if (!iterIdToLaunch || isNaN(iterIdToLaunch)) {
      return alert("Aucune itération configurée. Sauvegardez d'abord.");
    }

    setTriggering(true);
    setAirflowMsg(null);
    setDagRunId(null);
    
    try {
      const agentEmail = userEmail || "agent_inconnu";
      const response = await api.post("/iterations/trigger-airflow", {
        iterationId: iterIdToLaunch,
        triggeredBy: agentEmail,
      });

      if (response.data.success) {
        setDagRunId(response.data.dagRunId);
        setAirflowMsg({
          type: "success",
          text: `🚀 DAG lancé ! Run ID : ${response.data.dagRunId}`,
        });
      } else {
        setAirflowMsg({ type: "error", text: `❌ ${response.data.error}` });
      }
    } catch (e) {
      setAirflowMsg({ type: "error", text: "❌ Erreur : " + (e.response?.data?.error || e.message) });
    } finally {
      setTriggering(false);
    }
  };

  // ── Compteurs ────────────────────────────────────────────────
  
  const planModeACount = Object.keys(planModeA).length;
  const allTablesModeAChecked = areAllTablesSelectedModeA();
  const allRulesModeAChecked = areAllRulesSelectedForCurrentTable();

  // ─── RENDER ──────────────────────────────────────────────────

  return (
    <div className="page-with-sidebar">
      <SideBar role="AGENT" />

      <div className="main-content-iterconfig">

        {/* HEADER */}
        <div className="ic-header">
          <div className="ic-header-left">
            <h1 className="ic-title">📊 Configuration des itérations</h1>
            {userEmail && <span className="ic-user-badge">👤 {userEmail}</span>}
          </div>
          <button 
            className="ic-refresh-btn" 
            onClick={() => { loadSourceTables(); loadNextIterationId(); }} 
            title="Actualiser"
          >
            🔄
          </button>
        </div>

        {/* BANNERS */}
        {successMsg && <div className="ic-success-banner">{successMsg}</div>}
        {airflowMsg && (
          <div className={`ic-airflow-banner ${airflowMsg.type}`}>
            {airflowMsg.text}
            {dagRunId && <span className="ic-dagrun-id"> · DAG Run : <code>{dagRunId}</code></span>}
          </div>
        )}

        {/* LOADING */}
        {loading && (
          <div className="ic-loading">
            <div className="ic-spinner" />
            <p>Chargement…</p>
          </div>
        )}

        {/* ERROR */}
        {error && !loading && (
          <div className="ic-error">
            <span>⚠️</span>
            <p>{error}</p>
            <button onClick={() => { loadSourceTables(); loadNextIterationId(); }}>Réessayer</button>
          </div>
        )}

        {!loading && !error && (
          <>
            {/* Config ID + action bar */}
            <div className="ic-mode-a-bar">
              <div className="ic-mode-a-id-wrap">
                <label className="ic-selector-label">🆔 ID Itération</label>
                <div className="ic-fixed-id-display">
                  <span className="ic-fixed-id-value">{newIterationId || "..."}</span>
                  <span className="ic-fixed-id-badge">🔒 Auto</span>
                </div>
              </div>

              <div className="ic-mode-a-counter">
                <span className="ic-counter-number">{planModeACount}</span>
                <span className="ic-counter-label">règle(s) sélectionnée(s)</span>
              </div>

              <div className="ic-action-btns">
                <button
                  className="ic-btn ic-btn-deselect"
                  onClick={() => setPlanModeA({})}
                  disabled={planModeACount === 0}
                >
                  ✖️ Vider
                </button>
                <button
                  className="ic-btn ic-btn-export"
                  onClick={handleSavePlanModeA}
                  disabled={exporting || planModeACount === 0}
                >
                  {exporting ? "⏳ Sauvegarde..." : "💾 Sauvegarder itération"}
                </button>
                <button
                  className="ic-btn ic-btn-airflow"
                  onClick={handleTriggerAirflow}
                  disabled={triggering || !newIterationId}
                >
                  {triggering ? "⏳ Lancement..." : "🚀 Lancer Airflow"}
                </button>
              </div>
            </div>

            {/* Barre de sélection globale */}
            <div className="ic-mode-a-global-selection">
              <div className="ic-global-checks-mode-a">
                <label className="ic-global-check-item ic-global-check-item-large">
                  <input 
                    type="checkbox" 
                    checked={allTablesModeAChecked}
                    ref={input => {
                      if (input) input.indeterminate = hasSomeRulesInAnyTable() && !allTablesModeAChecked;
                    }}
                    onChange={(e) => e.target.checked ? selectAllTablesModeA() : deselectAllTablesModeA()}
                  />
                  <span>📋 Toutes les tables ({sourceTables.length})</span>
                </label>

                <div className="ic-selection-divider">|</div>

                <label className={`ic-global-check-item ${!selectedSourceTable ? 'disabled' : ''}`}>
                  <input 
                    type="checkbox" 
                    checked={allRulesModeAChecked}
                    disabled={!selectedSourceTable}
                    ref={input => {
                      if (input && selectedSourceTable) {
                        input.indeterminate = hasSomeRulesForCurrentTable() && !allRulesModeAChecked;
                      }
                    }}
                    onChange={(e) => e.target.checked ? selectAllRulesModeA() : deselectAllRulesModeA()}
                  />
                  <span>📐 Règles de cette table</span>
                </label>
              </div>
            </div>

            {/* Grille tables source */}
            <div className="ic-source-layout">

              {/* Panneau gauche : Tables */}
              <div className="ic-source-tables-panel">
                <h3 className="ic-panel-title">📋 Tables disponibles</h3>
                <div className="ic-source-table-list">
                  {sourceTables.length === 0 && (
                    <div className="ic-empty-small">Aucune table</div>
                  )}
                  {sourceTables.map(t => {
                    const count = Object.values(planModeA).filter(p => p.tableName === t.tableName).length;
                    return (
                      <div
                        key={t.tableName}
                        className={`ic-source-table-item ${selectedSourceTable === t.tableName ? "active" : ""}`}
                        onClick={() => loadRulesForTable(t.tableName)}
                      >
                        <div className="ic-source-table-name">🗄️ {t.tableName}</div>
                        <div className="ic-source-table-meta">
                          ~{(t.estimatedRows || 0).toLocaleString()} lignes
                          {count > 0 && <span className="ic-rules-in-plan-count">✓ {count} règle(s)</span>}
                        </div>
                        <label className="ic-batch-wrap-inline" onClick={e => e.stopPropagation()}>
                          <input
                            type="number"
                            className="ic-batch-input-small"
                            placeholder="Batch"
                            value={tableBatchSizes[t.tableName] || ""}
                            min={0}
                            onChange={e => setBatchSize(t.tableName, e.target.value)}
                          />
                        </label>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Panneau droit : Règles */}
              <div className="ic-source-rules-panel">
                {!selectedSourceTable && (
                  <div className="ic-source-rules-empty">
                    <p>👈 Sélectionnez une table</p>
                  </div>
                )}

                {selectedSourceTable && (
                  <>
                    <h3 className="ic-panel-title">
                      📐 Règles — <span className="ic-panel-table-name">{selectedSourceTable}</span>
                      <span className="ic-panel-rules-count">
                        ({Object.values(planModeA).filter(p => p.tableName === selectedSourceTable).length})
                      </span>
                    </h3>

                    {loadingRules && (
                      <div className="ic-loading-small">
                        <div className="ic-spinner" /> <p>Chargement…</p>
                      </div>
                    )}

                    {!loadingRules && sourceTableRules.length === 0 && (
                      <div className="ic-source-rules-empty">
                        <p>Aucune règle pour cette table</p>
                      </div>
                    )}

                    {!loadingRules && sourceTableRules.map(col => (
                      <div key={col.columnName} className="ic-col-block">
                        <div className="ic-col-header open">
                          <span className="ic-col-name">{col.columnName}</span>
                          <span className="ic-col-meta">{(col.rules || []).length} règle(s)</span>
                        </div>
                        
                        <div className="ic-rules-list">
                          {(col.rules || []).map(rule => {
                            const inPlan = isRuleInPlanA(selectedSourceTable, col.columnName, rule.ruleLabel);
                            return (
                              <div key={rule.id} className={`ic-rule-row ${inPlan ? "selected" : ""}`}>
                                <label className="ic-check-wrap">
                                  <input
                                    type="checkbox"
                                    checked={inPlan}
                                    onChange={() => toggleRuleModeA(
                                      selectedSourceTable, col.columnName, rule.ruleLabel, rule.id, rule.ruleType
                                    )}
                                  />
                                </label>
                                <div className="ic-rule-body">
                                  <div className="ic-rule-top">
                                    <code className="ic-rule-code">{rule.ruleLabel}</code>
                                    <span className="ic-rule-type-badge">{rule.ruleType}</span>
                                  </div>
                                  {rule.ruleDescription && <p className="ic-rule-desc">{rule.ruleDescription}</p>}
                                  <small className="ic-rule-id-display">#{rule.id}</small>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    ))}
                  </>
                )}
              </div>
            </div>

            {/* Résumé du plan */}
            {planModeACount > 0 && (
              <div className="ic-plan-summary">
                <h3 className="ic-panel-title">📋 Résumé — Itération {newIterationId}</h3>
                <div className="ic-plan-rows">
                  {Object.values(planModeA).map((entry, idx) => (
                    <div key={idx} className="ic-plan-row">
                      <span className="ic-plan-table">{entry.tableName}</span>
                      <span className="ic-plan-sep">›</span>
                      <span className="ic-plan-col">{entry.columnName}</span>
                      <span className="ic-plan-sep">›</span>
                      <code className="ic-plan-rule">{entry.ruleLabel}</code>
                      {entry.ruleId && <span className="ic-plan-ruleid-badge">#{entry.ruleId}</span>}
                      <button
                        className="ic-plan-remove"
                        onClick={() => toggleRuleModeA(entry.tableName, entry.columnName, entry.ruleLabel)}
                      >✕</button>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}