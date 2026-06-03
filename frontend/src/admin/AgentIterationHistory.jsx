import React, { useState, useEffect } from 'react';
import axios from 'axios';
import "./AgentIterationHistory.css";

export default function AgentIterationHistory() {
    const [agentsHistory, setAgentsHistory] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedAgent, setSelectedAgent] = useState(null);
    const [activeTab, setActiveTab] = useState('detection');
    const [iterations, setIterations] = useState([]);
    const [correctionIterations, setCorrectionIterations] = useState([]);
    const [loadingDetail, setLoadingDetail] = useState(false);

    useEffect(() => {
        fetchAgentsHistory();
    }, []);

    const fetchAgentsHistory = async () => {
        try {
            const response = await axios.get('/admin/agents/history-summary', {
                withCredentials: true
            });
            setAgentsHistory(response.data);
        } catch (error) {
            console.error('Erreur:', error);
        } finally {
            setLoading(false);
        }
    };

    const fetchAgentIterations = async (agent) => {
        if (selectedAgent?.id === agent.id) {
            setSelectedAgent(null);
            setIterations([]);
            setCorrectionIterations([]);
            return;
        }
        setSelectedAgent(agent);
        setActiveTab(null);
        setLoadingDetail(true);
        try {
            const [detectionRes, correctionRes] = await Promise.all([
                axios.get(`/admin/agents/${agent.id}/iterations`, { withCredentials: true }),
                axios.get(`/admin/agents/${agent.id}/correction-iterations`, { withCredentials: true })
            ]);
            setIterations(detectionRes.data);
            setCorrectionIterations(correctionRes.data);
        } catch (error) {
            console.error('Erreur détail:', error);
        } finally {
            setLoadingDetail(false);
        }
    };

    const groupDetectionIterations = (data) => {
        const grouped = data.reduce((acc, it) => {
            const key = it.iterationId;
            if (!acc[key]) {
                acc[key] = { iterationId: it.iterationId, runId: it.runId, dates: [] };
            }
            if (!acc[key].dates.includes(it.selectedAt)) {
                acc[key].dates.push(it.selectedAt);
            }
            return acc;
        }, {});
        return Object.values(grouped);
    };

    const groupCorrectionIterations = (data) => {
        // Filtrer : garder uniquement les lignes ayant un detectionRunId non vide
        const filtered = data.filter(it => it.detectionRunId && it.detectionRunId.trim() !== '' && it.detectionRunId !== '-');

        const grouped = filtered.reduce((acc, it) => {
            const key = it.iterationId;
            if (!acc[key]) {
                acc[key] = {
                    iterationId: it.iterationId,
                    dagRunId: it.dagRunId,
                    detectionRunId: it.detectionRunId,
                    createdAt: it.createdAt,
                    yesCount: 0,
                    noCount: 0
                };
            }
            if (it.toExecute === 'YES') acc[key].yesCount++;
            else acc[key].noCount++;
            return acc;
        }, {});

        // Trier décroissant par iterationId (le plus récent en premier)
        return Object.values(grouped).sort((a, b) => b.iterationId - a.iterationId);
    };

    const getInitials = (firstname, lastname) => {
        const f = firstname?.trim()?.[0] ?? '?';
        const l = lastname?.trim()?.[0] ?? '';
        return (f + l).toUpperCase();
    };

    if (loading) return <div className="loading">Chargement...</div>;

    return (
        <div className="container">
            <div className="header">
                <h1>Historique des Itérations</h1>
            </div>

            <div className="content">
                <table>
                    <thead>
                        <tr>
                            <th>Agent</th>
                            <th>Itérations (D / C)</th>
                            
                            <th>Détail</th>
                        </tr>
                    </thead>
                    <tbody>
                        {agentsHistory.map(agent => (
                            <React.Fragment key={agent.id}>
                                <tr
                                    className={`agent-row ${selectedAgent?.id === agent.id ? 'active' : ''}`}
                                    onClick={() => fetchAgentIterations(agent)}
                                    style={{ cursor: 'pointer' }}
                                >
                                    <td>
                                        <div className="agent-name">
                                            <div className="avatar">
                                                {getInitials(agent.firstname, agent.lastname)}
                                            </div>
                                            <div>
                                                <div>{agent.firstname} {agent.lastname}</div>
                                                <div className="agent-email">{agent.email}</div>
                                            </div>
                                        </div>
                                    </td>
                                    <td>
                                        <div className="badges-group">
                                            <span className="badge-detection" title="Itérations de détection">
                                                <i className="ti ti-search" aria-hidden="true"></i>
                                                {agent.totalDetectionIterations ?? 0}
                                            </span>
                                            <span className="badge-correction" title="Itérations de correction">
                                                <i className="ti ti-tool" aria-hidden="true"></i>
                                                {agent.totalCorrectionIterations ?? 0}
                                            </span>
                                        </div>
                                    </td>
                                    
                                    <td>
                                        <span className="toggle-icon">
                                            {selectedAgent?.id === agent.id ? '▲ ' : '▼Détail'}
                                        </span>
                                    </td>
                                </tr>

                                {selectedAgent?.id === agent.id && (
                                    <tr className="detail-row">
                                        <td colSpan={4}>
                                            {/* Onglets */}
                                            <div className="tabs">
                                                <button
                                                    className={`tab-btn ${activeTab === 'detection' ? 'tab-active' : ''}`}
                                                    onClick={(e) => { e.stopPropagation(); setActiveTab('detection'); }}
                                                >
                                                    Détection
                                                </button>
                                                <button
                                                    className={`tab-btn ${activeTab === 'correction' ? 'tab-active' : ''}`}
                                                    onClick={(e) => { e.stopPropagation(); setActiveTab('correction'); }}
                                                >
                                                    Correction
                                                </button>
                                            </div>

                                            {loadingDetail ? (
                                                <div className="detail-loading">Chargement des itérations...</div>
                                            ) : activeTab === 'detection' ? (
                                                <DetectionTab data={groupDetectionIterations(iterations)} />
                                            ) : activeTab === 'correction' ? (
                                                <CorrectionTab data={groupCorrectionIterations(correctionIterations)} />
                                            ) : (
                                                <div className="detail-empty">Sélectionnez un onglet pour afficher les détails.</div>
                                            )}
                                        </td>
                                    </tr>
                                )}
                            </React.Fragment>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

//  Onglet Détection 
function DetectionTab({ data }) {
    if (data.length === 0) return <div className="detail-empty">Aucune itération  de détection trouvée.</div>;
    return (
        <table className="inner-table">
            <thead>
                <tr>
                    <th>Iteration ID</th>
                    <th>Run ID</th>
                    <th>Dates d'exécution</th>
                </tr>
            </thead>
            <tbody>
                {data.map(group => (
                    <tr key={group.iterationId}>
                        <td className="mono">{group.iterationId}</td>
                        <td className="mono">{group.runId}</td>
                        <td>
                            {group.dates.map((date, i) => (
                                <div key={i} className="execution-date">{date}</div>
                            ))}
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}

//  Onglet Correction 
function CorrectionTab({ data }) {
    if (data.length === 0) return <div className="detail-empty">Aucune itération de correction trouvée.</div>;

    return (
        <table className="inner-table">
            <thead>
                <tr>
                    <th>Iteration ID</th>
                    <th>DAG Run ID</th>
                    <th>Detection Run</th>
                    <th>Date</th>
                </tr>
            </thead>
            <tbody>
                {data.map(group => (
                    <tr key={group.iterationId}>
                        <td className="mono">{group.iterationId}</td>
                        <td className="mono">{truncate(group.dagRunId, 25)}</td>
                        <td className="mono">{truncate(group.detectionRunId, 25)}</td>
                        <td>{group.createdAt}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}

function truncate(str, max) {
    if (!str) return '-';
    return str.length > max ? str.substring(0, max) + '…' : str;
}

function formatDate(dateString) {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleDateString('fr-FR', {
        day: '2-digit', month: '2-digit', year: 'numeric',
        hour: '2-digit', minute: '2-digit'
    });
}