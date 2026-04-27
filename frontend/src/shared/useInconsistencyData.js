// useInconsistencyData.js - Version corrigée avec filtrage par run
import { useEffect, useState, useCallback, useMemo } from 'react';
import axios from 'axios';

const api = axios.create({
    baseURL: '',  
    withCredentials: true,
    headers: { 'Content-Type': 'application/json' }
});
export function useInconsistencyData() {
    const [tables, setTables] = useState([]);
    const [tableData, setTableData] = useState([]);
    const [selectedTable, setSelectedTable] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    
    const [iterationsList, setIterationsList] = useState([]);
    const [selectedIterationId, setSelectedIterationId] = useState(null);
    const [selectedRunId, setSelectedRunId] = useState(null);
    const [runsForSelectedIteration, setRunsForSelectedIteration] = useState([]);
    const [summary, setSummary] = useState(null);
    const [fullTablesData, setFullTablesData] = useState([]);

    // Charger la liste des itérations
    useEffect(() => {
        const fetchIterations = async () => {
    try {
        const response = await api.get('/api/iterations/list');
        console.log("=== API /iterations/list RAW RESPONSE ===", response.data);
        
        const data = response.data;
        const iterations = data.iterations || [];
        console.log("=== ITERATIONS PARSED ===", iterations);
        
        setIterationsList(iterations);

        if (iterations.length > 0) {
            const first = iterations[0];
            console.log("=== FIRST ITERATION ===", first);
            console.log("=== RUNS ===", first.runs);
            setSelectedIterationId(first.iterationId);
            setRunsForSelectedIteration(first.runs || []);
            if (first.runs && first.runs.length > 0) {
                setSelectedRunId(first.runs[0].runId);
            }
        }
    } catch (err) {
        console.error("=== ERREUR fetchIterations ===", err.response?.status, err.response?.data, err.message);
    }
};
        fetchIterations();
    }, []);

    // ✅ Charger les résultats pour un run spécifique
    const fetchResultsByRun = useCallback(async (iterationId, runId) => {
        if (!iterationId || !runId) return;
        
        setLoading(true);
        setError(null);
        
        try {
            console.log(`📡 Chargement itération ${iterationId}, run ${runId}...`);
            const response = await api.get(`/api/inconsistencies/iteration/${iterationId}/run/${runId}`);
                        const result = response.data;
            
            console.log(`📥 Résultat pour run ${runId}:`, result);
            
            if (result.found) {
                // ✅ Les données sont directement dans result.data
                const inconsistencies = result.data || [];
                
                // ✅ Grouper par table pour l'affichage
                const tablesMap = new Map();
                
                for (const inc of inconsistencies) {
                    const tableName = inc.tableName;
                    const columnName = inc.columnName;
                    
                    if (!tablesMap.has(tableName)) {
                        tablesMap.set(tableName, {
                            tableName: tableName,
                            totalViolations: 0,
                            columns: new Map()
                        });
                    }
                    
                    const table = tablesMap.get(tableName);
                    
                    if (!table.columns.has(columnName)) {
                        table.columns.set(columnName, {
                            columnName: columnName,
                            totalViolations: 0,
                            rules: []
                        });
                    }
                    
                    const column = table.columns.get(columnName);
                    
                    const ruleEntry = {
                        id: inc.id,
                        rule: inc.rule,
                        ruleDescription: inc.ruleDescription,
                        errorCategory: inc.errorCategory,
                        countSource: inc.countSource,
                        nbViolations: inc.nbViolations,
                        nbToCorrect: inc.nbToCorrect,
                        nbToMigrate: inc.nbToMigrate,
                        tauxRejet: inc.tauxRejet,
                        executionDate: inc.executionDate,
                        runId: inc.runId
                    };
                    
                    column.rules.push(ruleEntry);
                    column.totalViolations += (inc.nbViolations || 0);
                    table.totalViolations += (inc.nbViolations || 0);
                }
                
                // Convertir les Maps en tableaux
                const formattedTables = Array.from(tablesMap.values()).map(table => ({
                    tableName: table.tableName,
                    totalViolations: table.totalViolations,
                    columns: Object.fromEntries(table.columns)
                }));
                
                setFullTablesData(formattedTables);
                
                // ✅ Extraire les noms des tables
                const tableNames = formattedTables.map(table => table.tableName);
                setTables(tableNames);
                
                if (tableNames.length > 0 && !selectedTable) {
                    setSelectedTable(tableNames[0]);
                } else if (tableNames.length > 0 && selectedTable && !tableNames.includes(selectedTable)) {
                    setSelectedTable(tableNames[0]);
                }
                
                setSummary({
                    totalRules: inconsistencies.length,
                    totalViolations: inconsistencies.reduce((sum, inc) => sum + (inc.nbViolations || 0), 0),
                    tablesCount: formattedTables.length,
                    executionDate: result.executionDate,
                    runId: result.runId
                });
                
            } else {
                console.log(`⚠️ Aucune donnée pour run ${runId}`);
                setFullTablesData([]);
                setTables([]);
                setSelectedTable('');
                setSummary(null);
            }
        } catch (err) {
            console.error(`❌ Erreur chargement run ${runId}:`, err);
            setError(err.message);
            setFullTablesData([]);
            setTables([]);
            setSelectedTable('');
        } finally {
            setLoading(false);
        }
    }, [selectedTable]);

    // ✅ Recharger quand le run change
    useEffect(() => {
        if (selectedIterationId && selectedRunId) {
            fetchResultsByRun(selectedIterationId, selectedRunId);
        }
    }, [selectedIterationId, selectedRunId, fetchResultsByRun]);

    // ✅ Filtrer les données par table sélectionnée
    useEffect(() => {
        if (!selectedTable || fullTablesData.length === 0) {
            setTableData([]);
            return;
        }
        
        const selectedTableData = fullTablesData.find(
            table => table.tableName === selectedTable
        );
        
        if (selectedTableData) {
            const formattedData = [];
            const columns = Object.values(selectedTableData.columns || {});
            
            for (const column of columns) {
                const rules = column.rules || [];
                for (const rule of rules) {
                    formattedData.push({
                        tableName: selectedTableData.tableName,
                        columnName: column.columnName,
                        ...rule
                    });
                }
            }
            setTableData(formattedData);
        } else {
            setTableData([]);
        }
    }, [selectedTable, fullTablesData]);

    // Changer d'itération
    const handleIterationChange = useCallback((iterationId) => {
        const selectedIter = iterationsList.find(i => i.iterationId === parseInt(iterationId));
        if (selectedIter) {
            console.log(`🔄 Changement vers itération ${iterationId}`);
            setSelectedIterationId(selectedIter.iterationId);
            setRunsForSelectedIteration(selectedIter.runs || []);
            if (selectedIter.runs && selectedIter.runs.length > 0) {
                setSelectedRunId(selectedIter.runs[0].runId);
            } else {
                setSelectedRunId(null);
            }
            // Réinitialiser les données
            setFullTablesData([]);
            setTables([]);
            setSelectedTable('');
            setTableData([]);
        }
    }, [iterationsList]);

    // Changer de run
    const handleRunChange = useCallback((runId) => {
        console.log(`🔄 Changement vers run ${runId}`);
        setSelectedRunId(runId);
        // Réinitialiser les données
        setFullTablesData([]);
        setTables([]);
        setSelectedTable('');
        setTableData([]);
    }, []);

    // Calculer les stats pour la table sélectionnée
    const aggregatedStats = useMemo(() => {
        if (!tableData || tableData.length === 0) {
            return {
                countSource: 0,
                nbToCorrect: 0,
                nbToMigrate: 0,
                tauxRejet: 0
            };
        }
        
        const firstItem = tableData[0];
        return {
            countSource: firstItem?.countSource || 0,
            nbToCorrect: firstItem?.nbToCorrect || 0,
            nbToMigrate: firstItem?.nbToMigrate || 0,
            tauxRejet: firstItem?.tauxRejet || 0
        };
    }, [tableData]);

    return {
        tables,
        tableData,
        selectedTable,
        setSelectedTable,
        loading,
        error,
        stats: aggregatedStats,
        iterationsList,
        selectedIterationId,
        setSelectedIterationId: handleIterationChange,
        runsForSelectedIteration,
        selectedRunId,
        setSelectedRunId: handleRunChange,
        summary
    };
}