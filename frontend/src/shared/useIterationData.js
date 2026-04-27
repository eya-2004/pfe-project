// useIterationData.js
import { useState, useEffect, useCallback } from 'react';

export function useIterationData(selectedTable, baseData) {
    const [iterations, setIterations] = useState([]);
    const [selectedIteration, setSelectedIteration] = useState('latest');
    const [filteredData, setFilteredData] = useState(null);
    const [loadingIterations, setLoadingIterations] = useState(false);

    // Générer des itérations à partir des données existantes
    useEffect(() => {
        if (!baseData || baseData.length === 0) {
            setIterations([]);
            return;
        }

        // Extraire les dates uniques des itérations
        const iterationMap = new Map();
        
        baseData.forEach(item => {
            if (item.iteration_id || item.analysis_date) {
                const iterId = item.iteration_id || `iter_${item.analysis_date}`;
                if (!iterationMap.has(iterId)) {
                    iterationMap.set(iterId, {
                        id: iterId,
                        label: `Analyse ${iterationMap.size + 1}`,
                        date: item.analysis_date || item.date,
                        time: item.analysis_time || '00:00:00',
                        timestamp: new Date(item.analysis_date || item.date).getTime()
                    });
                }
            }
        });

        // Trier par date décroissante
        const sortedIterations = Array.from(iterationMap.values())
            .sort((a, b) => b.timestamp - a.timestamp);
        
        setIterations(sortedIterations);
    }, [baseData]);

    // Filtrer les données par itération sélectionnée
    useEffect(() => {
        if (!baseData) {
            setFilteredData(null);
            return;
        }

        setLoadingIterations(true);
        
        setTimeout(() => {
            if (selectedIteration === 'latest') {
                // Prendre les données les plus récentes
                const latestTimestamp = Math.max(...baseData.map(d => 
                    new Date(d.analysis_date || d.date || 0).getTime()
                ));
                const filtered = baseData.filter(d => 
                    new Date(d.analysis_date || d.date || 0).getTime() === latestTimestamp
                );
                setFilteredData(filtered);
            } else {
                // Filtrer par itération spécifique
                const filtered = baseData.filter(d => 
                    (d.iteration_id === selectedIteration) ||
                    (d.analysis_date === iterations.find(i => i.id === selectedIteration)?.date)
                );
                setFilteredData(filtered);
            }
            setLoadingIterations(false);
        }, 100);
    }, [selectedIteration, baseData, iterations]);

    // Filtrage personnalisé par date/heure
    const filterByDateTime = useCallback((date, time) => {
        if (!baseData || !date) return;
        
        setLoadingIterations(true);
        
        const filterDate = new Date(date);
        const hasTime = time && time.length > 0;
        
        let filtered = baseData.filter(item => {
            const itemDate = new Date(item.analysis_date || item.date);
            
            // Comparer les dates
            const sameDate = itemDate.toDateString() === filterDate.toDateString();
            
            if (!sameDate) return false;
            
            // Si une heure est spécifiée, comparer aussi l'heure
            if (hasTime) {
                const [hours, minutes, seconds] = time.split(':');
                const itemHours = itemDate.getHours();
                const itemMinutes = itemDate.getMinutes();
                
                return itemHours === parseInt(hours) && 
                       itemMinutes === parseInt(minutes);
            }
            
            return true;
        });
        
        // Créer une itération temporaire pour ce filtre
        const customIteration = {
            id: `custom_${date}_${time || 'all'}`,
            label: `Filtre personnalisé`,
            date: date,
            time: time || 'Toute la journée',
            timestamp: filterDate.getTime(),
            isCustom: true
        };
        
        setIterations(prev => {
            // Éviter les doublons
            const exists = prev.some(i => i.id === customIteration.id);
            if (!exists && (date || time)) {
                return [customIteration, ...prev];
            }
            return prev;
        });
        
        setFilteredData(filtered);
        setSelectedIteration(customIteration.id);
        setLoadingIterations(false);
    }, [baseData]);

    return {
        iterations,
        selectedIteration,
        setSelectedIteration,
        filteredData,
        loadingIterations,
        filterByDateTime
    };
}