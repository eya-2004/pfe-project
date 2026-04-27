import React from 'react';
import Icon from './Icon';

function KpiCard({ icon, label, value, suffix = '' }) {
    return (
        <div className="kpi-card">
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

export default function KpiGrid({ stats = {}, tableData = [], selectedTable = '' }) {
    
    // ✅ AUCUN CALCUL - On prend les valeurs directement du DAG
    const lignesSource = stats?.countSource || 0;
    const aCorriger = stats?.nbToCorrect || 0;
    const aMigrer = stats?.nbToMigrate || 0;
    const tauxRejet = stats?.tauxRejet || 0;
    
    const nbRegles = Array.isArray(tableData) ? tableData.length : 0;

    return (
        <div className="kpi-grid">
            
            {/* 📊 CARTE 1 : Lignes Source */}
            <KpiCard
                icon={<Icon name="database" size={20} />}
                label="Lignes Source"
                value={lignesSource}
            />

            {/* ⚠️ CARTE 2 : À Corriger */}
            <KpiCard
                icon={<Icon name="warning" size={20} />}
                label="À Corriger"
                value={aCorriger}
            />

            {/* ✅ CARTE 3 : À Migrer */}
            <KpiCard
                icon={<Icon name="check" size={20} />}
                label="À Migrer"
                value={aMigrer}
            />

            {/* 📈 CARTE 4 : Taux de Rejet */}
            <KpiCard
                icon={<Icon name="chart" size={20} />}
                label="Taux de Rejet"
                value={tauxRejet}
                suffix="%"
            />

            {/* 📋 CARTE 5 : Règles Analysées */}
            <KpiCard
                icon={<Icon name="list" size={20} />}
                label="Règles Vérifiées"
                value={nbRegles}
            />
            
        </div>
    );
}