import React, { useState } from 'react';
import { useInconsistencyData } from './useInconsistencyData';
import DashboardHeader from './DashboardHeader';
import KpiGrid from './KpiGrid';
import ChartsView from './ChartsView';
import CategoryBarChart from './CategoryBarChart';
import TableView from './TableView';
import Icon from './Icon';
import "./dashboard.css";

export default function DetectedInconsistency({ role }) {
    const [viewMode, setViewMode] = useState('charts');

    const {
        tables,
        tableData,
        selectedTable, setSelectedTable,
        loading, error,
        stats,
        iterationsList,
        selectedIterationId, setSelectedIterationId,
        runsForSelectedIteration,
        selectedRunId, setSelectedRunId,
        correctionMode, setCorrectionMode,
        hasCorrectionData,
        summary,
        noDataAtAll,        
        noDataForIteration 
    } = useInconsistencyData();
    /* remplace nbViolations par nbViolationsAfter  si mose correction after */
    const filteredTableData = tableData.map(item => {
        if (correctionMode === 'after' && hasCorrectionData) {
            return {
                ...item,
                nbViolations: item.nbViolationsAfter ?? item.nbViolations,
                nbToCorrect:  item.nbToCorrectAfter  ?? item.nbToCorrect,
                nbToMigrate:  item.nbToMigrateAfter  ?? item.nbToMigrate,
                tauxRejet:    item.tauxRejetAfter     ?? item.tauxRejet,
            };
        }
        return item;
    });

    const tableStats = {
        countSource:      filteredTableData[0]?.countSource      || 0,
        nbToCorrect:      filteredTableData[0]?.nbToCorrect      || 0,
        nbToMigrate:      filteredTableData[0]?.nbToMigrate      || 0,
        tauxRejet:        filteredTableData[0]?.tauxRejet        || 0,
        nbToCorrectAfter: filteredTableData[0]?.nbToCorrectAfter ?? null,
        nbToMigrateAfter: filteredTableData[0]?.nbToMigrateAfter ?? null,
        tauxRejetAfter:   filteredTableData[0]?.tauxRejetAfter   ?? null,
    };

    return (
        <div className="professional-dashboard">
            <DashboardHeader
                tables={tables}
                selectedTable={selectedTable}
                onTableChange={setSelectedTable}
                viewMode={viewMode}
                onViewModeChange={setViewMode}
                loading={loading}
                iterationsList={iterationsList}
                selectedIterationId={selectedIterationId}
                onIterationChange={setSelectedIterationId}
                runsForSelectedIteration={runsForSelectedIteration}
                selectedRunId={selectedRunId}
                onRunChange={setSelectedRunId}
                summary={summary}
                correctionMode={correctionMode}
                onCorrectionModeChange={setCorrectionMode}
                hasCorrectionData={hasCorrectionData}
                role={role}
            />

            {loading && (
                <div className="loading-state">
                    <div className="loading-spinner" />
                    <p>Chargement des données...</p>
                </div>
            )}

            {error && (
                <div className="error-state">
                    <span className="error-icon">
                        <Icon name="warning" size={18} />
                    </span>
                    <div className="error-content">
                        <h3>Erreur de chargement</h3>
                        <p>{error}</p>
                    </div>
                    <button className="retry-btn" onClick={() => window.location.reload()}>
                        Réessayer
                    </button>
                </div>
            )}

            {!loading && !error && noDataAtAll && (
                <div className="empty-state">
                    <span className="empty-icon">
                        <Icon name="inbox" size={22} />
                    </span>
                    <h3>Aucune donnée disponible</h3>
                    <p>Aucune itération trouvée dans le système.</p>
                </div>
            )}

            {!loading && !error && !noDataAtAll && noDataForIteration && (
                <div className="empty-state">
                    <span className="empty-icon">
                        <Icon name="inbox" size={22} />
                    </span>
                    <h3>Aucune donnée pour l'itération sélectionnée</h3>
                    <p>
                        {selectedTable
                            ? `Aucune donnée trouvée pour l'itération ${selectedIterationId} et la table ${selectedTable}.`
                            : `L'itération ${selectedIterationId} ne contient aucune donnée.`
                        }
                    </p>
                </div>
            )}

            {!loading && !error && !noDataAtAll && !noDataForIteration && (
                <>
                    <KpiGrid
                        stats={tableStats}
                        tableData={filteredTableData}
                        selectedTable={selectedTable}
                        correctionMode={correctionMode}
                        hasCorrectionData={hasCorrectionData}
                    />

                    {viewMode === 'charts' && (
                        <ChartsView
                            tableData={filteredTableData}
                            correctionMode={correctionMode}
                            hasCorrectionData={hasCorrectionData}
                        />
                    )}
                    {viewMode === 'categories' && (
                        <CategoryBarChart tableData={filteredTableData} />
                    )}
                    {viewMode === 'table' && (
                        <TableView
                            tableData={filteredTableData}
                            selectedTable={selectedTable}
                            correctionMode={correctionMode}
                            hasCorrectionData={hasCorrectionData}
                        />
                    )}
                </>
            )}
        </div>
    );
}