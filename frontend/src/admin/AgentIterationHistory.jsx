import React, { useState, useEffect } from 'react';
import axios from 'axios';
import "./AgentIterationHistory.css";

export default function AgentIterationHistory() {
    const [agentsHistory, setAgentsHistory] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedAgent, setSelectedAgent] = useState(null);
    const [iterations, setIterations] = useState([]);
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
            return;
        }
        setSelectedAgent(agent);
        setLoadingDetail(true);
        try {
            const response = await axios.get(`/admin/agents/${agent.id}/iterations`, {
                withCredentials: true
            });
            setIterations(response.data);
        } catch (error) {
            console.error('Erreur détail:', error);
        } finally {
            setLoadingDetail(false);
        }
    };
     // Après avoir reçu les iterations, les grouper par iterationId
const groupedIterations = iterations.reduce((acc, it) => {
    const key = it.iterationId;
    if (!acc[key]) {
        acc[key] = { iterationId: it.iterationId, runId: it.runId, dates: [] };
    }
    acc[key].dates.push(it.selectedAt);
    return acc;
}, {});

const groupedList = Object.values(groupedIterations);
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
                            <th>Total Itérations</th>
                            <th>Dernière Itération</th>
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
                                        <span className="badge">{agent.totalIterations}</span>
                                    </td>
                                    <td>{formatDate(agent.lastIterationDate)}</td>
                                    <td>
                                        <span className="toggle-icon">
                                            {selectedAgent?.id === agent.id ? '▲' : '▼'}
                                        </span>
                                    </td>
                                </tr>

                                {selectedAgent?.id === agent.id && (
                                <tr className="detail-row">
                                    <td colSpan={4}>
                                        {loadingDetail ? (
                                            <div className="detail-loading">Chargement des itérations...</div>
                                        ) : iterations.length === 0 ? (
                                            <div className="detail-empty">Aucune itération trouvée.</div>
                                        ) : (() => {
                                            const groupedIterations = iterations.reduce((acc, it) => {
                                                const key = it.iterationId;
                                                if (!acc[key]) {
                                                    acc[key] = { iterationId: it.iterationId, runId: it.runId, dates: [] };
                                                }
                                                // selectedAt est déjà une String formatée depuis le backend
                                                if (!acc[key].dates.includes(it.selectedAt)) {
                                                    acc[key].dates.push(it.selectedAt);
                                                }
                                                return acc;
                                            }, {});
                                            const groupedList = Object.values(groupedIterations);

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
                                                        {groupedList.map(group => (
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
                                        })()}
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

function formatDate(dateString) {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleDateString('fr-FR', {
        day: '2-digit', month: '2-digit', year: 'numeric',
        hour: '2-digit', minute: '2-digit'
    });
}