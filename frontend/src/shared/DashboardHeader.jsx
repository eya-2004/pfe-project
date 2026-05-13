import React , { useState } from 'react';
import Icon from './Icon';
import {useNavigate} from 'react-router-dom';
export default function DashboardHeader({
    tables,
    selectedTable, onTableChange,
    viewMode, onViewModeChange,
    loading,
    // Props pour les itérations
    iterationsList = [],
    selectedIterationId, onIterationChange,
    runsForSelectedIteration = [],
    selectedRunId, onRunChange,
     correctionMode,
    onCorrectionModeChange,
    hasCorrectionData,
    role,
    summary = null
}) {
    // Trouver le run sélectionné
    const selectedRun = runsForSelectedIteration.find(run => run.runId === selectedRunId);
    const [showNoCorrection, setShowNoCorrection] = useState(false);
    const navigate = useNavigate();
    return (
        <>
            <header className="dashboard-header">
                <div className="nav-brand">
                    <span className="brand-icon">
                        <Icon name="logo" size={18} />
                    </span>
                    <span className="brand-name">DataQuality</span>
                    <span className="brand-badge">Monitor</span>
                </div>
                <div className="nav-actions">
                    <button
                        className={`nav-btn ${viewMode === 'charts' ? 'active' : ''}`}
                        onClick={() => onViewModeChange('charts')}
                    >
                        <span className="btn-icon">
                            <Icon name="chart" size={16} />
                        </span>
                        Graphiques
                    </button>
                    <button
                        className={`nav-btn ${viewMode === 'categories' ? 'active' : ''}`}
                        onClick={() => onViewModeChange('categories')}
                    >
                        <span className="btn-icon">
                            <Icon name="tag" size={16} />
                        </span>
                        Par catégorie
                    </button>
                    <button
                        className={`nav-btn ${viewMode === 'table' ? 'active' : ''}`}
                        onClick={() => onViewModeChange('table')}
                    >
                        <span className="btn-icon">
                            <Icon name="table" size={16} />
                        </span>
                        Tableau
                    </button>
                </div>
            </header>
            <div className="filters-main-bar">
                <div className="filters-row">
                
                    <div className="filter-group">
                        <label className="filter-label">
                            <Icon name="refresh" size={14} />
                            Mode affichage
                        </label>
                        <div className="correction-toggle">
                            <button
                                className={`toggle-btn ${correctionMode === 'before' ? 'active' : ''}`}
                                onClick={() => onCorrectionModeChange('before')}
                            >
                                ⚡ Avant correction
                            </button>
                            <button
                                className={`toggle-btn ${correctionMode === 'after' ? 'active' : ''}`}
                                onClick={() => {
                                    if (!hasCorrectionData) {
                                        setShowNoCorrection(true); // 👈 ouvrir la fenêtre
                                    } else {
                                        onCorrectionModeChange('after');
                                    }
                                }}
                            >
                                ✅ Après correction
                            </button>
                        </div>
                    </div>
                    {/* Sélecteur de table — à ajouter dans filters-row */}
                    <div className="filter-group">
                        <label className="filter-label">
                            <Icon name="table" size={14} />
                            Table
                        </label>
                        <select
                            className="filter-select"
                            value={selectedTable || ''}
                            onChange={e => onTableChange(e.target.value)}
                            disabled={loading || tables.length === 0}
                        >
                            {tables.length === 0 && (
                                <option value="">Aucune table disponible</option>
                            )}
                            {tables.map(table => (
                                <option key={table} value={table}>
                                    {table}
                                </option>
                            ))}
                        </select>
                    </div>
                    {/* Sélecteur d'itération */}
                    <div className="filter-group">
                        <label className="filter-label">
                            <Icon name="history" size={14} />
                            Itération
                        </label>
                        <select
                            className="filter-select"
                            value={selectedIterationId || ''}
                            onChange={e => onIterationChange(e.target.value)}
                            disabled={loading || iterationsList.length === 0}
                        >
                            {iterationsList.map(iter => (
                                <option key={iter.iterationId} value={iter.iterationId}>
                                    Itération {iter.iterationId}
                                </option>
                            ))}
                        </select>
                    </div>

                    {/* Sélecteur de run (date/heure) */}
                    {runsForSelectedIteration.length > 0 && (
                        <div className="filter-group">
                            <label className="filter-label">
                                <Icon name="clock" size={14} />
                                Date / Heure d'exécution
                            </label>
                            <select
                                className="filter-select"
                                value={selectedRunId || ''}
                                onChange={e => onRunChange(e.target.value)}
                                disabled={loading}
                            >
                                {runsForSelectedIteration.map((run, index) => (
                                    <option key={run.runId} value={run.runId}>
                                        {new Date(run.executionDate).toLocaleString('fr-FR', {
                                            day: '2-digit',
                                            month: '2-digit',
                                            year: 'numeric',
                                            hour: '2-digit',
                                            minute: '2-digit'
                                        })}
                                        {index === 0 && ' (plus récente)'}
                                    </option>
                                ))}
                            </select>
                        </div>
                    )}
                </div>

            </div>
                        {showNoCorrection && (
                <div className="modal-overlay" onClick={() => setShowNoCorrection(false)}>
                    <div className="modal-box" onClick={e => e.stopPropagation()}>
                        <div className="modal-icon">⚠️</div>
                        <h3>Aucune correction effectuée</h3>
                        <p>
                            {role === 'admin' 
                                ? "Aucune itération corrective n'a été effectuée par les agents de migration pour cette itération."
                                : "Cette itération n'a pas encore fait l'objet d'une correction. Veuillez configurer et lancer une itération corrective."
                            }
                        </p>
                        <div className="modal-actions">
                            {role !== 'admin' && (
                                <button 
                                    className="modal-btn-primary"
                                    onClick={() => {
                                        setShowNoCorrection(false);
                                        navigate('/agent/correctionConfig');
                                    }}
                                >
                                    Configurer une itération
                                </button>
                            )}
                            <button 
                                className="modal-btn-secondary"
                                onClick={() => setShowNoCorrection(false)}
                            >
                                Fermer
                            </button>
                        </div>
                    </div>
                </div>
            )}
                        
        </>
    );
}