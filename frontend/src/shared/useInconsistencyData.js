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
    const [correctionMode, setCorrectionMode] = useState('before');

    // Flag pour savoir si aucune donnée n'existe du tout
    const [noDataAtAll, setNoDataAtAll] = useState(false);
    // Flag pour savoir si aucune donnée pour l'itération sélectionnée
    const [noDataForIteration, setNoDataForIteration] = useState(false);

    const selectedTableRef = useRef('');
    useEffect(() => { selectedTableRef.current = selectedTable; }, [selectedTable]);
    const processAndSetData = useCallback((inconsistencies, iterationId, runId) => {
        if (!inconsistencies || inconsistencies.length === 0) {
            setTables([]);
            setSelectedTable('');
            setTableData([]);
            setSummary(null);
            return;
        }

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

        const tableNames = Array.from(tablesMap.keys());
        setTables(tableNames);

        // Conserver la table sélectionnée si elle existe encore
        const currentTable = selectedTableRef.current;
        if (!currentTable || !tableNames.includes(currentTable)) {
            setSelectedTable(tableNames[0]);
        }

        setSummary({
            totalRules: inconsistencies.length,
            totalViolations: inconsistencies.reduce((sum, inc) => sum + (inc.nbViolations || 0), 0),
            tablesCount: tableNames.length,
            iterationId,
            runId
        });
    }, []);


    useEffect(() => {
        const fetchInitialData = async () => {
            setLoading(true);
            setError(null);
            setNoDataAtAll(false);
            try {
                // Récupérer la liste des itérations
                const iterResponse = await api.get('/api/iterations/list');
                const iterations = iterResponse.data.iterations || [];
                setIterationsList(iterations);

                if (iterations.length === 0) {
                    setNoDataAtAll(true);
                    setLoading(false);
                    return;
                }

                const lastIteration = iterations[0];
                setSelectedIterationId(lastIteration.iterationId);
                setRunsForSelectedIteration(lastIteration.runs || []);
                if (lastIteration.runs?.length > 0) {
                    setSelectedRunId(lastIteration.runs[0].runId);
                }

                const dataResponse = await api.get(
                    `/api/inconsistencies/iteration/${lastIteration.iterationId}`
                );

                if (!dataResponse.data || dataResponse.data.length === 0) {
                    setNoDataForIteration(true);
                } else {
                    setNoDataForIteration(false);
                    processAndSetData(dataResponse.data, lastIteration.iterationId, null);
                }

            } catch (err) {
                console.error('Erreur chargement initial:', err.message);
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchInitialData();
    }, [processAndSetData]);
    useEffect(() => {
        if (!selectedIterationId || !selectedTable) {
            setTableData([]);
            return;
        }

        let cancelled = false;

        const fetchByTable = async () => {
            setLoading(true);
            setError(null);
            try {
                const response = await api.get('/api/inconsistencies/filter', {
                    params: {
                        iterationId: selectedIterationId,
                        tableName: selectedTable
                    }
                });

                if (cancelled) return;

                const data = response.data || [];

                if (data.length === 0) {
                    setNoDataForIteration(true);
                    setTableData([]);
                } else {
                    setNoDataForIteration(false);
                    // Aplatir directement pour tableData 
                    setTableData(data.map(inc => ({
                        tableName: inc.tableName,
                        columnName: inc.columnName,
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
                    })));
                }
            } catch (err) {
                if (cancelled) return;
                console.error('Erreur filtre par table:', err.message);
                setError(err.message);
                setTableData([]);
            } finally {
                if (!cancelled) setLoading(false);
            }
        };

        fetchByTable();
        return () => { cancelled = true; };

    }, [selectedIterationId, selectedTable]);

    useEffect(() => {
        if (!selectedIterationId || !selectedRunId) return;

        let cancelled = false;

        const fetchByRun = async () => {
            setLoading(true);
            setError(null);
            try {
                const response = await api.get(
                    `/api/inconsistencies/iteration/${selectedIterationId}/run/${selectedRunId}`
                );

                if (cancelled) return;

                const result = response.data;
                if (result.found && result.data?.length > 0) {
                    processAndSetData(result.data, selectedIterationId, selectedRunId);
                    setNoDataForIteration(false);
                } else {
                    setNoDataForIteration(true);
                    setTables([]);
                    setSelectedTable('');
                    setTableData([]);
                    setSummary(null);
                }
            } catch (err) {
                if (cancelled) return;
                console.error('Erreur filtre par run:', err.message);
                setError(err.message);
                setTables([]);
                setSelectedTable('');
                setTableData([]);
            } finally {
                if (!cancelled) setLoading(false);
            }
        };

        fetchByRun();
        return () => { cancelled = true; };

    }, [selectedIterationId, selectedRunId, processAndSetData]);
    const handleIterationChange = useCallback((iterationId) => {
        const selectedIter = iterationsList.find(i => i.iterationId === parseInt(iterationId));
        if (selectedIter) {
            setSelectedIterationId(selectedIter.iterationId);
            setRunsForSelectedIteration(selectedIter.runs || []);
            setSelectedRunId(selectedIter.runs?.[0]?.runId || null);
            setTables([]);
            setSelectedTable('');
            setTableData([]);
            setNoDataForIteration(false);
        }
    }, [iterationsList]);
    const handleRunChange = useCallback((runId) => {
        setSelectedRunId(runId);
        setTables([]);
        setSelectedTable('');
        setTableData([]);
        setNoDataForIteration(false);
    }, []);

    const hasCorrectionData = useMemo(() => {
        const iter = iterationsList.find(i => i.iterationId === selectedIterationId);
        return iter?.hasCorrection === true;
    }, [iterationsList, selectedIterationId]);
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
        summary,
        noDataAtAll,        
        noDataForIteration 
    };
}