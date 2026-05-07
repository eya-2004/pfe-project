import React from 'react';
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell,
    PieChart, Pie, Legend,
    LabelList 
} from 'recharts';

// Tooltip personnalisé pour l'histogramme
function CustomBarTooltip({ active, payload }) {
    if (!active || !payload?.length) return null;
    const d = payload[0].payload;
    return (
        <div className="custom-tooltip">
            <p className="tooltip-label"><strong>{d.name}</strong></p>
            <p className="tooltip-rule">Règle : {d.rule}</p>
            <p className="tooltip-violations">Violations : {d.nbViolations}</p>
        </div>
    );
}

// Tooltip personnalisé pour le donut
function CustomDonutTooltip({ active, payload }) {
    if (!active || !payload?.length) return null;
    const d = payload[0].payload;
    return (
        <div className="custom-tooltip">
            <p className="tooltip-label"><strong>{d.name}</strong></p>
            <p className="tooltip-value">Nombre : {d.value.toLocaleString()}</p>
        </div>
    );
}

export default function ChartsView({ tableData }) {
    
    // Données pour l'histogramme - Filtre des colonnes sans erreurs
    const barChartData = React.useMemo(() => {
        if (!tableData || !Array.isArray(tableData)) return [];
        
        return tableData
            .map(item => ({
                name: item.columnName || '-',
                nbViolations: item.nbViolations || 0,
                rule: item.rule || '-',
                category: item.errorCategory || '-'
            }))
            .filter(item => item.nbViolations > 0)  // Garder seulement les erreurs
            .sort((a, b) => b.nbViolations - a.nbViolations)
            .slice(0, 8);  // Top 8 des colonnes AVEC erreurs
    }, [tableData]);

    // Données pour Donut Chart - LIGNES CORRECTES vs LIGNES À CORRIGER
   // Données pour Donut Chart
const donutData = React.useMemo(() => {
    if (!tableData || !Array.isArray(tableData)) return [];
    
    const nbToCorrect = tableData[0]?.nbToCorrect ?? 0;
    const nbToMigrate = tableData[0]?.nbToMigrate ?? 0;
    
    return [
        { name: 'Lignes à corriger', value: nbToCorrect, color: '#141b64' },
        { name: 'Lignes à migrer',   value: nbToMigrate, color: '#5c77a3' }
    ];
}, [tableData]);

const totalLignes = donutData.reduce((sum, item) => sum + item.value, 0);
    // Message si aucune erreur détectée
    const hasErrors = barChartData.length > 0;

    if (!tableData || tableData.length === 0) {
        return (
            <div className="charts-view-empty">
                <p>📊 Aucune donnée à afficher</p>
            </div>
        );
    }

    return (
        <div className="charts-view">
            <div className="charts-grid">
                
                {/* ==================== HISTOGRAMME ==================== */}
                <div className="chart-container">
                    <div className="chart-header">
                        <h3 className="chart-title">
                            <span className="title-dot" />
                            Violations par règle
                        </h3>
                        <span className="chart-note">
                            {hasErrors 
                                ? `Top ${barChartData.length} colonnes avec violations` 
                                : '✅ Aucune violation détectée'}
                        </span>
                    </div>
                    
                    <div className="chart-body">
                        {hasErrors ? (
                            <ResponsiveContainer width="100%" height={400}>
                                <BarChart
                                    data={barChartData}
                                    layout="horizontal"
                                    margin={{ 
                                        top: 20, 
                                        right: 30, 
                                        left: 120,
                                        bottom: 60 
                                    }}
                                    barCategoryGap="20%"
                                    barGap={8}
                                >
                                    <CartesianGrid 
                                        strokeDasharray="3 3" 
                                        stroke="#E5E7EB" 
                                        horizontal={true} 
                                        vertical={true}
                                        strokeOpacity={0.6}
                                    />
                                    
                                    {/* Axe Y */}
                                    <YAxis 
                                        type="number" 
                                        tick={{ fontSize: 12, fill: '#6B7280' }}
                                        axisLine={{ stroke: '#D1D5DB', strokeWidth: 1.5 }}
                                        tickLine={{ stroke: '#D1D5DB', strokeWidth: 1, length: 6 }}
                                        label={{
                                            value: 'Nombre de Violations',
                                            angle: -90,
                                            position: 'insideLeft',
                                            offset: -70,
                                            style: { fontSize: 13, fontWeight: 600, fill: '#374151' }
                                        }}
                                        allowDecimals={false}
                                        domain={[0, 'auto']}
                                    />
                                    
                                    {/* Axe X */}
                                    <XAxis 
                                        type="category" 
                                        dataKey="name" 
                                        width={100}
                                        tick={{ fontSize: 11, fill: '#374151', fontWeight: 500 }}
                                        axisLine={{ stroke: '#D1D5DB', strokeWidth: 1.5 }}
                                        tickLine={{ stroke: '#D1D5DB', strokeWidth: 1, length: 6 }}
                                        interval={0}
                                        textAnchor="end"
                                        angle={-15}
                                        height={80}
                                        label={{
                                            value: 'Colonnes',
                                            position: 'bottom',
                                            offset: 45,
                                            style: { fontSize: 13, fontWeight: 600, fill: '#374151' }
                                        }}
                                    />
                                    
                                    <Tooltip content={<CustomBarTooltip />} />
                                    
                                    <Bar
                                        dataKey="nbViolations"
                                        radius={[6, 6, 0, 0]}
                                        barSize={28}
                                        name="Violations"
                                        maxBarSize={50}
                                    >
                                        {barChartData.map((entry, index) => (
                                            <Cell 
                                                key={`cell-${index}`} 
                                                fill={
                                                    entry.nbViolations > 100 ? '#DC2626' :
                                                    entry.nbViolations > 50  ? '#EF4444' :
                                                    entry.nbViolations > 10  ? '#F87171' :
                                                    '#FCA5A5'
                                                } 
                                                stroke="none"
                                            />
                                        ))}
                                        
                                        <LabelList 
                                            dataKey="nbViolations" 
                                            position="top"
                                            offset={5}
                                            style={{ fontSize: 11, fontWeight: 700, fill: '#DC2626' }}
                                            formatter={(value) => 
                                                value > 0 ? value.toLocaleString('fr-FR') : ''
                                            }
                                        />
                                    </Bar>
                                    
                                    <Legend 
                                        verticalAlign="top" 
                                        align="right"
                                        iconType="circle"
                                        iconSize={10}
                                    />
                                </BarChart>
                            </ResponsiveContainer>
                        ) : (
                            // Message quand tout est OK
                            <div className="no-errors-message" style={{
                                display: 'flex',
                                flexDirection: 'column',
                                alignItems: 'center',
                                justifyContent: 'center',
                                height: '400px',
                                color: '#059669',
                                backgroundColor: '#ECFDF5',
                                borderRadius: '12px',
                                border: '2px dashed #A7F3D0'
                            }}>
                                <span style={{ fontSize: '48px', marginBottom: '16px' }}>✅</span>
                                <span style={{ fontSize: '18px', fontWeight: 700 }}>Parfait !</span>
                                <span style={{ fontSize: '14px', marginTop: '8px' }}>
                                    Aucune violation détectée dans les données
                                </span>
                            </div>
                        )}
                    </div>
                </div>
<div className="chart-container">
    <div className="chart-header">
        <h3 className="chart-title">
            <span className="title-dot" />
            Lignes à corriger vs à migrer
        </h3>
        <span className="chart-note">Total : {totalLignes.toLocaleString()} lignes</span>
    </div>
    
    <div className="chart-body donut-chart-body">
        <ResponsiveContainer width="100%" height={300}>
            <PieChart>
                <Pie
                    data={donutData}
                    cx="50%"
                    cy="50%"
                    innerRadius={70}
                    outerRadius={110}
                    paddingAngle={2}
                    dataKey="value"
                    label={({ name, value }) => `${name}: ${value.toLocaleString()}`}
                    labelLine={{ stroke: '#9CA3AF', strokeWidth: 1 }}
                >
                    {donutData.map((entry, index) => (
                        <Cell 
                            key={`cell-${index}`} 
                            fill={entry.color}
                            stroke="none"
                        />
                    ))}
                </Pie>
                <Tooltip content={<CustomDonutTooltip />} />
                <Legend 
                    verticalAlign="bottom" 
                    height={36}
                    iconType="circle"
                    formatter={(value) => <span style={{ color: '#374151', fontSize: '13px', fontWeight: 500 }}>{value}</span>}
                />
            </PieChart>
        </ResponsiveContainer>
    </div>
    
    <div className="donut-summary">
        <div className="donut-summary-item correct">
            <span className="summary-label">✓ À corriger</span>
            <span className="summary-value">{donutData[0]?.value.toLocaleString() || 0}</span>
        </div>
        <div className="donut-summary-item incorrect">
            <span className="summary-label">✗ À migrer</span>
            <span className="summary-value">{donutData[1]?.value.toLocaleString() || 0}</span>
        </div>
    </div>

        </div></div></div>
    );
}