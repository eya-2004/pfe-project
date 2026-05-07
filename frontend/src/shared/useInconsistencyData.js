// useInconsistencyData.js - Version corrigée
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
    const [correctionMode, setCorrectionMode] = useState('before');

    // Charger la liste des itérations
    useEffect(() => {
        const fetchIterations = async () => {
            try {
                const response = await api.get('/api/iterations/list');
                const data = response.data;
                const iterations = data.iterations || [];
                setIterationsList(iterations);

                if (iterations.length > 0) {
                    const first = iterations[0];
                    setSelectedIterationId(first.iterationId);
                    setRunsForSelectedIteration(first.runs || []);
                    if (first.runs && first.runs.length > 0) {
                        setSelectedRunId(first.runs[0].runId);
                    }
                }
            } catch (err) {
                console.error("Erreur fetchIterations:", err.message);
            }
        };
        fetchIterations();
    }, []);

    // Charger les résultats pour un run spécifique
    const fetchResultsByRun = useCallback(async (iterationId, runId) => {
        if (!iterationId || !runId) return;
        
        setLoading(true);
        setError(null);
        
        try {
            const response = await api.get(`/api/inconsistencies/iteration/${iterationId}/run/${runId}`);
            const result = response.data;
            
            if (result.found) {
                const inconsistencies = result.data || [];
                const tablesMap = new Map();
                
                for (const inc of inconsistencies) {
                    const tableName = inc.tableName;
                    const columnName = inc.columnName;
                    
                    if (!tablesMap.has(tableName)) {
                        tablesMap.set(tableName, {
                            tableName,
                            totalViolations: 0,
                            columns: new Map()
                        });
                    }
                    
                    const table = tablesMap.get(tableName);
                    
                    if (!table.columns.has(columnName)) {
                        table.columns.set(columnName, {
                            columnName,
                            totalViolations: 0,
                            rules: []
                        });
                    }
                    
                    const column = table.columns.get(columnName);
                    
                    // ✅ ruleEntry propre, sans doublon
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
                        nbViolationsAfter: inc.nbViolationsAfter ?? null,
                        nbToCorrectAfter:  inc.nbToCorrectAfter  ?? null,
                        nbToMigrateAfter:  inc.nbToMigrateAfter  ?? null,   // ← nouveau
                        tauxRejetAfter:    inc.tauxRejetAfter     ?? null,
                        // Skipped
                        isSkipped:         inc.isSkipped          ?? false,  // ← nouveau
                        skipReason:        inc.skipReason         ?? null,   // ← nouveau

                        executionDate: inc.executionDate,
                        runId: inc.runId
                    };
                    
                    column.rules.push(ruleEntry);
                    column.totalViolations += (inc.nbViolations || 0);
                    table.totalViolations += (inc.nbViolations || 0);
                }
                
                const formattedTables = Array.from(tablesMap.values()).map(table => ({
                    tableName: table.tableName,
                    totalViolations: table.totalViolations,
                    columns: Object.fromEntries(table.columns)
                }));
                
                setFullTablesData(formattedTables);
                
                const tableNames = formattedTables.map(t => t.tableName);
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
                setFullTablesData([]);
                setTables([]);
                setSelectedTable('');
                setSummary(null);
            }
        } catch (err) {
            console.error('Erreur chargement run:', err);
            setError(err.message);
            setFullTablesData([]);
            setTables([]);
            setSelectedTable('');
        } finally {
            setLoading(false);
        }
    }, [selectedTable]);

    // Recharger quand le run change
    useEffect(() => {
        if (selectedIterationId && selectedRunId) {
            fetchResultsByRun(selectedIterationId, selectedRunId);
        }
    }, [selectedIterationId, selectedRunId, fetchResultsByRun]);

    // Filtrer les données par table sélectionnée
    useEffect(() => {
        if (!selectedTable || fullTablesData.length === 0) {
            setTableData([]);
            return;
        }
        
        const selectedTableData = fullTablesData.find(t => t.tableName === selectedTable);
        
        if (selectedTableData) {
            const formattedData = [];
            const columns = Object.values(selectedTableData.columns || {});
            for (const column of columns) {
                for (const rule of (column.rules || [])) {
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
            setSelectedIterationId(selectedIter.iterationId);
            setRunsForSelectedIteration(selectedIter.runs || []);
            setSelectedRunId(selectedIter.runs?.[0]?.runId || null);
            setFullTablesData([]);
            setTables([]);
            setSelectedTable('');
            setTableData([]);
        }
    }, [iterationsList]);

    // Changer de run
    const handleRunChange = useCallback((runId) => {
        setSelectedRunId(runId);
        setFullTablesData([]);
        setTables([]);
        setSelectedTable('');
        setTableData([]);
    }, []);

    const hasCorrectionData = useMemo(() => {
        if (!fullTablesData || fullTablesData.length === 0) return false;
        
        // Chercher dans toutes les tables et colonnes
        for (const table of fullTablesData) {
            const columns = Object.values(table.columns || {});
            for (const col of columns) {
                for (const rule of (col.rules || [])) {
                    if (
                        !rule.isSkipped &&
                        rule.nbViolationsAfter !== null &&
                        rule.nbViolationsAfter !== undefined
                    ) {
                        return true;
                    }
                }
            }
        }
        return false;
    }, [fullTablesData]);
    const aggregatedStats = useMemo(() => {
        if (!tableData || tableData.length === 0) {
            return {
                countSource: 0, nbToCorrect: 0, nbToMigrate: 0, tauxRejet: 0,
                nbToCorrectAfter: null, nbToMigrateAfter: null, tauxRejetAfter: null,
            };
        }

        const firstItem = tableData[0];

        // Chercher nbToCorrectAfter sur N'IMPORTE quel item (skipped ou pas)
        const firstWithAfter = tableData.find(
            item => item.nbToCorrectAfter !== null &&
                    item.nbToCorrectAfter !== undefined
        );

        console.log("=== aggregatedStats ===");
        console.log("firstItem:", firstItem);
        console.log("firstWithAfter:", firstWithAfter);

        return {
            countSource:      firstItem?.countSource || 0,
            nbToCorrect:      firstItem?.nbToCorrect || 0,
            nbToMigrate:      firstItem?.nbToMigrate || 0,
            tauxRejet:        firstItem?.tauxRejet   || 0,
            nbToCorrectAfter: firstWithAfter?.nbToCorrectAfter ?? null,
            nbToMigrateAfter: firstWithAfter?.nbToMigrateAfter ?? null,
            tauxRejetAfter:   firstWithAfter?.tauxRejetAfter   ?? null,
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
        correctionMode,
        setCorrectionMode,
        hasCorrectionData,
        summary
    };
}