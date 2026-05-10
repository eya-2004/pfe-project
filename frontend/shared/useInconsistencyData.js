// useInconsistencyData.js
import { useEffect, useState, useCallback, useMemo, useRef } from 'react';
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

    // Refs pour lire les valeurs courantes sans créer de dépendances
    const selectedTableRef = useRef('');
    useEffect(() => { selectedTableRef.current = selectedTable; }, [selectedTable]);

    // Charger la liste des itérations (une seule fois)
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
                console.error('Erreur fetchIterations:', err.message);
            }
        };
        fetchIterations();
    }, []);

    // Fonction de fetch pure — prend iterationId et runId en paramètres
    // Ne dépend d'aucun state → pas de boucle, pas de closure stale
    const processAndSetData = useCallback((inconsistencies, iterationId, runId) => {
        const tablesMap = new Map();

        for (const inc of inconsistencies) {
            const { tableName, columnName } = inc;

            if (!tablesMap.has(tableName)) {
                tablesMap.set(tableName, { tableName, totalViolations: 0, columns: new Map() });
            }
            const table = tablesMap.get(tableName);

            if (!table.columns.has(columnName)) {
                table.columns.set(columnName, { columnName, totalViolations: 0, rules: [] });
            }
            const column = table.columns.get(columnName);

            column.rules.push({
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
                nbToCorrectAfter: inc.nbToCorrectAfter ?? null,
                nbToMigrateAfter: inc.nbToMigrateAfter ?? null,
                tauxRejetAfter: inc.tauxRejetAfter ?? null,
                isSkipped: inc.isSkipped ?? false,
                skipReason: inc.skipReason ?? null,
                executionDate: inc.executionDate,
                runId: inc.runId
            });

            column.totalViolations += (inc.nbViolations || 0);
            table.totalViolations += (inc.nbViolations || 0);
        }

        const formattedTables = Array.from(tablesMap.values()).map(t => ({
            tableName: t.tableName,
            totalViolations: t.totalViolations,
            columns: Object.fromEntries(t.columns)
        }));

        setFullTablesData(formattedTables);

        const tableNames = formattedTables.map(t => t.tableName);
        setTables(tableNames);

        // Conserver la table sélectionnée si elle existe encore
        const currentTable = selectedTableRef.current;
        if (tableNames.length > 0) {
            if (!currentTable || !tableNames.includes(currentTable)) {
                setSelectedTable(tableNames[0]);
            }
        }

        setSummary({
            totalRules: inconsistencies.length,
            totalViolations: inconsistencies.reduce((sum, inc) => sum + (inc.nbViolations || 0), 0),
            tablesCount: formattedTables.length,
            iterationId,
            runId
        });
    }, []);

    // useEffect déclenché uniquement par selectedIterationId / selectedRunId
    useEffect(() => {
        if (!selectedIterationId || !selectedRunId) return;

        let cancelled = false; // évite les race conditions si on change vite

        const doFetch = async () => {
            setLoading(true);
            setError(null);
            try {
                const response = await api.get(
                    `/api/inconsistencies/iteration/${selectedIterationId}/run/${selectedRunId}`
                );
                if (cancelled) return;

                const result = response.data;
                if (result.found) {
                    processAndSetData(result.data || [], selectedIterationId, selectedRunId);
                } else {
                    setFullTablesData([]);
                    setTables([]);
                    setSelectedTable('');
                    setSummary(null);
                }
            } catch (err) {
                if (cancelled) return;
                console.error('Erreur chargement run:', err);
                setError(err.message);
                setFullTablesData([]);
                setTables([]);
                setSelectedTable('');
            } finally {
                if (!cancelled) setLoading(false);
            }
        };

        doFetch();

        return () => { cancelled = true; }; // cleanup si le composant change avant la fin
    }, [selectedIterationId, selectedRunId, processAndSetData]);

    // Filtrer par table sélectionnée
    useEffect(() => {
        if (!selectedTable || fullTablesData.length === 0) {
            setTableData([]);
            return;
        }

        const selectedTableData = fullTablesData.find(t => t.tableName === selectedTable);
        if (selectedTableData) {
            const formattedData = [];
            for (const column of Object.values(selectedTableData.columns || {})) {
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

    // Changer d'itération — reset complet
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

    // Changer de run — conserver la table via la ref
    const handleRunChange = useCallback((runId) => {
        setSelectedRunId(runId);
        setFullTablesData([]);
        setTables([]);
        setTableData([]);
        // pas de setSelectedTable('') — la ref gère ça dans processAndSetData
    }, []);

    const hasCorrectionData = useMemo(() => {
        for (const table of fullTablesData) {
            for (const col of Object.values(table.columns || {})) {
                for (const rule of (col.rules || [])) {
                    if (!rule.isSkipped && rule.nbViolationsAfter !== null && rule.nbViolationsAfter !== undefined) {
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
        const firstWithAfter = tableData.find(
            item => item.nbToCorrectAfter !== null && item.nbToCorrectAfter !== undefined
        );
        return {
            countSource: firstItem?.countSource || 0,
            nbToCorrect: firstItem?.nbToCorrect || 0,
            nbToMigrate: firstItem?.nbToMigrate || 0,
            tauxRejet: firstItem?.tauxRejet || 0,
            nbToCorrectAfter: firstWithAfter?.nbToCorrectAfter ?? null,
            nbToMigrateAfter: firstWithAfter?.nbToMigrateAfter ?? null,
            tauxRejetAfter: firstWithAfter?.tauxRejetAfter ?? null,
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