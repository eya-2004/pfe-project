import React from 'react';
import Icon from './Icon';

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
    summary = null
}) {
    // Trouver le run sélectionné
    const selectedRun = runsForSelectedIteration.find(run => run.runId === selectedRunId);

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

            {/* Barre de filtres principale */}
            <div className="filters-main-bar">
                <div className="filters-row">
                    {/* Sélecteur de table */}
                    <div className="filter-group">
                        <label className="filter-label">
                            <Icon name="database" size={14} />
                            Table analysée
                        </label>
                        <select
                            className="filter-select"
                            value={selectedTable}
                            onChange={e => onTableChange(e.target.value)}
                            disabled={loading}
                        >
                            {tables.map(t => (
                                <option key={t} value={t}>{t}</option>
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

            {/* Banner d'information compact */}
            
        </>
    );
}