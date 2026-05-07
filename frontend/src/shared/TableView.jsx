import React from 'react';
import Icon from './Icon';

export default function TableView({ tableData, selectedTable, correctionMode, hasCorrectionData }) {
    
    if (!tableData || !Array.isArray(tableData) || tableData.length === 0) {
        return (
            <div className="tv-empty-state">
                <Icon name="inbox" size={32} />
                <h3>Aucune donnée disponible</h3>
                <p>Aucune incohérence détectée pour la table <strong>{selectedTable}</strong></p>
            </div>
        );
    }

    const isAfter = correctionMode === 'after' && hasCorrectionData;

    const getStatusBadge = (item) => {
        if (!isAfter) {
            return item.nbViolations > 0
                ? <span className="status-badge status-error">⚠️ Violations</span>
                : <span className="status-badge status-ok">✅ OK</span>;
        }
        if (item.isSkipped) {
            const reasonLabel = {
                'NO_BATCH':    'Aucun batch',
                'NO_PK':       'PK inconnue',
                'BUILD_FAILED':'Requête échouée',
            }[item.skipReason] || item.skipReason || '';
            return (
                <span className="status-badge status-skipped" title={item.skipReason || ''}>
                    ⏭ Ignorée{reasonLabel ? ` — ${reasonLabel}` : ''}
                </span>
            );
        }
        return item.nbViolationsAfter > 0
            ? <span className="status-badge status-error">⚠️ Violations restantes</span>
            : <span className="status-badge status-ok">✅ Corrigée</span>;
    };

    const violBg = (item) => {
        const val = isAfter ? item.nbViolationsAfter : item.nbViolations;
        return (val ?? 0) > 0 ? '#FEE2E2' : 'transparent';
    };

    return (
        <div className="tv-container">
            <div className="tv-header">
                <h2 className="tv-title">📋 Détail des règles — {selectedTable}</h2>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <span className="tv-count">{tableData.length} règle(s) vérifiée(s)</span>
                    {isAfter && (
                        <span style={{
                            fontSize: '0.8em', background: '#dbeafe',
                            color: '#1d4ed8', padding: '2px 10px',
                            borderRadius: '12px', fontWeight: 600
                        }}>
                            ✅ Mode après correction
                        </span>
                    )}
                </div>
            </div>

            <div className="tv-table-wrapper">
                <table className="tv-table">
                    <thead>
                        <tr>
                            <th>Colonne</th>
                            <th>Règle</th>
                            <th>Description</th>
                            <th>Catégorie</th>
                            {!isAfter && <th>Nombre de violation par règle</th>}
                            {isAfter && <th>Nombre de violation par règle</th>}
                            {!isAfter && <th>Statut</th>}
                            {isAfter && <th>Raison ignorée</th>}
                        </tr>
                    </thead>
                    <tbody>
                        {tableData.map((item, index) => (
                            <tr
                                key={item.id || index}
                                style={{ opacity: isAfter && item.isSkipped ? 0.6 : 1 }}
                            >
                                <td className="col-name"><strong>{item.columnName || '-'}</strong></td>
                                <td className="col-rule"><code>{item.rule || '-'}</code></td>
                                <td className="col-desc">{item.ruleDescription || '-'}</td>
                                <td className="col-cat"><span className="cat-badge">{item.errorCategory || '-'}</span></td>

                                {!isAfter && (
                                    <td className="col-num col-violation" style={{
                                        fontWeight: 'bold',
                                        backgroundColor: item.nbViolations > 0 ? '#FEE2E2' : 'transparent'
                                    }}>
                                        {item.nbViolations ?? '-'}
                                    </td>
                                )}

                                {isAfter && (
                                    <td className="col-num col-violation" style={{
                                        fontWeight: 'bold',
                                        backgroundColor: violBg(item)
                                    }}>
                                        {item.isSkipped ? '—' : (item.nbViolationsAfter ?? '—')}
                                    </td>
                                )}

                                {!isAfter && <td>{getStatusBadge(item)}</td>}

                                {isAfter && (
                                    <td className="col-skip-reason" style={{ color: '#6b7280', fontSize: '0.85em' }}>
                                        {item.isSkipped ? (item.skipReason || '—') : '—'}
                                    </td>
                                )}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            <div className="tv-footer">
                <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '15px' }}>
                    <span>{tableData.length} règle(s) chargée(s)</span>
                    <div style={{ fontSize: '0.85em', opacity: '0.8', display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                        <strong>Légende :</strong>
                        {!isAfter && <span style={{ color: '#DC2626' }}>● Violations = erreurs détectées</span>}
                        {isAfter && (
                            <>
                                <span style={{ color: '#DC2626' }}>● Violations après = erreurs restantes</span>
                                <span style={{ color: '#6b7280' }}>● ⏭ Ignorée = règle non exécutée</span>
                            </>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}