// DetectedInconsistency.jsx - Version corrigée
import React, { useState } from 'react';
import { useInconsistencyData } from './useInconsistencyData';
import DashboardHeader from './DashboardHeader';
import KpiGrid from './KpiGrid';
import ChartsView from './ChartsView';
import CategoryBarChart from './CategoryBarChart';
import TableView from './TableView';
import Icon from './Icon';
import "./dashboard.css";

export default function DetectedInconsistency() {
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
        summary
    } = useInconsistencyData();

    // Filtrer les données par table sélectionnée
    const filteredTableData = tableData.filter(item => 
        item.tableName === selectedTable
    ).map(item => {
        if (correctionMode === 'after' && hasCorrectionData) {
            return {
                ...item,
                nbViolations: item.nbViolationsAfter ?? item.nbViolations,
                nbToCorrect:  item.nbToCorrectAfter  ?? item.nbToCorrect,
                tauxRejet:    item.tauxRejetAfter     ?? item.tauxRejet,
            };
        }
        return item;
    });

    console.log("=== DETECTED INCONSISTENCY DEBUG ===");
    console.log("Itérations disponibles:", iterationsList);
    console.log("Itération sélectionnée:", selectedIterationId);
    console.log("Table sélectionnée:", selectedTable);
    console.log("Nombre de données:", filteredTableData.length);

    // Obtenir les statistiques pour la table sélectionnée
    // DetectedInconsistency.jsx
    const tableStats = {
        countSource:      filteredTableData[0]?.countSource      || 0,
        nbToCorrect:      filteredTableData[0]?.nbToCorrect      || 0,
        nbToMigrate:      filteredTableData[0]?.nbToMigrate      || 0,
        tauxRejet:        filteredTableData[0]?.tauxRejet        || 0,
        // ✅ Ajouter les trois clés manquantes
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

            {!loading && !error && (
                <>
                    <KpiGrid
                        stats={tableStats}
                        tableData={filteredTableData}
                        selectedTable={selectedTable}
                        correctionMode={correctionMode}
                        hasCorrectionData={hasCorrectionData}
                    />
                    
                    {viewMode === 'charts' && <ChartsView tableData={filteredTableData} />}
                    {viewMode === 'categories' && <CategoryBarChart tableData={filteredTableData} />}
                    {viewMode === 'table' && <TableView tableData={filteredTableData}
                                                        selectedTable={selectedTable}
                                                        correctionMode={correctionMode}
                                                        hasCorrectionData={hasCorrectionData}
                                                    />}
                </>
            )}

            {!loading && !error && filteredTableData.length === 0 && selectedTable && (
                <div className="empty-state">
                    <span className="empty-icon">
                        <Icon name="inbox" size={22} />
                    </span>
                    <h3>Aucune donnée disponible</h3>
                    <p>
                        {selectedIterationId 
                            ? `Aucune donnée trouvée pour l'itération ${selectedIterationId} et la table ${selectedTable}`
                            : "Sélectionnez une itération pour voir les résultats"}
                    </p>
                </div>
            )}
        </div>
    );
}