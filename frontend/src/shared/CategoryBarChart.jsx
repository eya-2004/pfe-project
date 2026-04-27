import React, { useMemo } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid,
    Tooltip, Cell, ResponsiveContainer, LabelList
} from 'recharts';

/* ── Couleur selon nombre de violations ── */
function getBarColor(value, maxViolations) {
    const ratio = value / maxViolations;
    if (ratio < 0.2) return '#059669';
    if (ratio < 0.5) return '#d97706';
    return '#dc2626';
}

/* ── Tooltip personnalisé ── */
function CustomTooltip({ active, payload }) {
    if (!active || !payload?.length) return null;
    const d = payload[0].payload;
    return (
        <div className="custom-tooltip">
            <p className="tooltip-label">{d.category}</p>
            <div className="tooltip-values">
                <span className="tooltip-incorrect">
                    🚨 Violations : {d.nbViolations}
                </span>
                <span style={{ color: '#94a3b8', fontSize: 11 }}>
                    {d.count} règle{d.count > 1 ? 's' : ''}
                </span>
            </div>
        </div>
    );
}

export default function CategoryBarChart({ tableData }) {
    const barData = useMemo(() => {
        if (!tableData || tableData.length === 0) return [];

        // Grouper par catégorie d'erreur
        const groups = {};
        tableData.forEach(item => {
            const cat = (item.errorCategory && item.errorCategory.trim())
                ? item.errorCategory
                : '(Sans catégorie)';
            if (!groups[cat]) {
                groups[cat] = {
                    totalViolations: 0,
                    count: 0
                };
            }
            groups[cat].totalViolations += (item.nbViolations || 0);
            groups[cat].count += 1;
        });

        // Convertir en tableau et trier par nombre de violations décroissant
        const data = Object.entries(groups)
            .map(([category, values]) => ({
                category,
                nbViolations: values.totalViolations,
                count: values.count
            }))
            .sort((a, b) => b.nbViolations - a.nbViolations);

        const maxViolations = data.length > 0 ? Math.max(...data.map(d => d.nbViolations)) : 1;

        return data;
    }, [tableData]);

    if (!barData.length) return null;

    const maxViolations = Math.max(...barData.map(d => d.nbViolations));

    return (
        <div className="charts-view" style={{ marginTop: 24 }}>
            <div className="chart-container">
                <div className="chart-header">
                    <h3 className="chart-title">
                        <span className="title-dot" />
                        Nombre de violations par catégorie d'erreur
                    </h3>
                    <span className="chart-note">Trié par nombre de violations décroissant</span>
                </div>

                <div className="chart-body" style={{ height: 400 }}>
                    <ResponsiveContainer width="100%" height="100%">
                        <BarChart
                            data={barData}
                            layout="horizontal"
                            margin={{ top: 20, right: 30, left: 40, bottom: 60 }}
                        >
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            
                            <XAxis 
                                dataKey="category" 
                                tick={{ fontSize: 12, fill: '#475569', angle: -45, textAnchor: 'end' }}
                                height={80}
                                interval={0}
                            />
                            
                            <YAxis 
                                tickFormatter={v => `${v}`}
                                tick={{ fontSize: 11, fill: '#475569' }}
                            />
                            
                            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(15,23,42,0.04)' }} />
                            
                            <Bar dataKey="nbViolations" radius={[6, 6, 0, 0]} barSize={40}>
                                {barData.map((entry, i) => (
                                    <Cell key={i} fill={getBarColor(entry.nbViolations, maxViolations)} fillOpacity={0.85} />
                                ))}
                                <LabelList 
                                    dataKey="nbViolations" 
                                    position="top" 
                                    formatter={v => `${v}`}
                                    style={{ fontSize: 11, fontWeight: 700, fill: '#0f172a' }}
                                />
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </div>

                {/* Légende */}
                <div style={{ display: 'flex', gap: 20, padding: '10px 18px 16px', flexWrap: 'wrap' }}>
                    {[
                        { color: '#059669', label: 'Faible (< 20% du max)' },
                        { color: '#d97706', label: 'Moyen (20-50% du max)' },
                        { color: '#dc2626', label: 'Élevé (> 50% du max)' },
                    ].map(({ color, label }) => (
                        <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                            <span style={{ width: 10, height: 10, borderRadius: '50%', background: color, display: 'inline-block' }} />
                            <span style={{ fontSize: 11, color: '#475569', fontWeight: 600 }}>{label}</span>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}