import React from 'react';
import Icon from './Icon';

function KpiCard({ icon, label, value, suffix = '', highlight = false }) {
    return (
        <div className="kpi-card" style={highlight ? { border: '2px solid #3b82f6', background: '#eff6ff' } : {}}>
            <div className="kpi-icon">{icon}</div>
            <div className="kpi-content">
                <span className="kpi-label">{label}</span>
                <span className="kpi-value">
                    {typeof value === 'number' ? value.toLocaleString('fr-FR') : value}
                    {suffix}
                </span>
            </div>
        </div>
    );
}
export default function KpiGrid({ stats = {}, tableData = [], selectedTable = '', correctionMode = 'before', hasCorrectionData = false }) {

    const isAfter = correctionMode === 'after' && hasCorrectionData;
    
    console.log("=== KPIGRID ===");
    console.log("correctionMode:", correctionMode);
    console.log("hasCorrectionData:", hasCorrectionData);
    console.log("isAfter:", isAfter);
    console.log("stats:", stats);
    const lignesSource = stats?.countSource  || 0;
    const aCorriger    = stats?.nbToCorrect  || 0;
    const aMigrer      = stats?.nbToMigrate  || 0;
    const tauxRejet    = stats?.tauxRejet    || 0;

    // Valeurs APRÈS (du DAG via stats when isAfter)
    const aCorrigerAfter = stats?.nbToCorrectAfter  ?? null;
    const aMigrerAfter   = stats?.nbToMigrateAfter  ?? null;
    const tauxAfter      = stats?.tauxRejetAfter     ?? null;

    const nbRegles = Array.isArray(tableData) ? tableData.length : 0;
    const nbSkipped = isAfter ? tableData.filter(r => r.isSkipped).length : 0;

    return (
        <div className="kpi-grid">

            <KpiCard
                icon={<Icon name="database" size={20} />}
                label="Lignes Source"
                value={lignesSource}
            />

            <KpiCard
                icon={<Icon name="warning" size={20} />}
                label={isAfter ? 'À corriger ' : 'À Corriger'}
                value={isAfter ? (aCorrigerAfter ?? '—') : aCorriger}
                highlight={isAfter}
            />

            <KpiCard
                icon={<Icon name="check" size={20} />}
                label={isAfter ? 'À Migrer ' : 'À Migrer'}
                value={isAfter ? (aMigrerAfter ?? '—') : aMigrer}
                highlight={isAfter}
            />

            <KpiCard
                icon={<Icon name="chart" size={20} />}
                label={isAfter ? 'Taux rejet ' : 'Taux de Rejet'}
                value={isAfter ? (tauxAfter ?? '—') : tauxRejet}
                suffix={isAfter ? (tauxAfter !== null ? '%' : '') : '%'}
                highlight={isAfter}
            />

            <KpiCard
                icon={<Icon name="list" size={20} />}
                label="Règles Vérifiées"
                value={nbRegles}
            />

            {isAfter && nbSkipped > 0 && (
                <KpiCard
                    icon={<Icon name="warning" size={20} />}
                    label="Règles ignorées"
                    value={nbSkipped}
                />
            )}

        </div>
    );
}