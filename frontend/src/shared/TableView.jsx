import React from 'react';
import Icon from './Icon';

export default function TableView({ tableData, selectedTable }) {
    
    if (!tableData || !Array.isArray(tableData) || tableData.length === 0) {
        return (
            <div className="tv-empty-state">
                <Icon name="inbox" size={32} />
                <h3>Aucune donnée disponible</h3>
                <p>Aucune incohérence détectée pour la table <strong>{selectedTable}</strong></p>
            </div>
        );
    }

    return (
        <div className="tv-container">
            
            <div className="tv-header">
                <h2 className="tv-title">📋 Détail des règles - {selectedTable}</h2>
                <span className="tv-count">{tableData.length} règle(s) vérifiée(s)</span>
            </div>

            <div className="tv-table-wrapper">
                <table className="tv-table">
                    <thead>
                        <tr>
                            <th>Colonne</th>
                            <th>Règle</th>
                            <th>Description</th>
                            <th>Catégorie</th>
                            <th>Nb Violations</th>
                        </tr>
                    </thead>
                    <tbody>
                        {tableData.map((item, index) => (
                            <tr key={item.id || index}>
                                <td className="col-name">
                                    <strong>{item.columnName || '-'}</strong>
                                </td>
                                <td className="col-rule">
                                    <code>{item.rule || '-'}</code>
                                </td>
                                <td className="col-desc">
                                    {item.ruleDescription || '-'}
                                </td>
                                <td className="col-cat">
                                    <span className="cat-badge">
                                        {item.errorCategory || '-'}
                                    </span>
                                </td>
                                <td className="col-num col-violation" style={{
                                    fontWeight: 'bold',
                                    backgroundColor: item.nbViolations > 0 ? '#FEE2E2' : 'transparent'
                                }}>
                                    {item.nbViolations ?? '-'}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            <div className="tv-footer">
                <div style={{display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '15px'}}>
                    <span>{tableData.length} règle(s) chargée(s) depuis la base de données</span>
                    <div style={{fontSize: '0.85em', opacity: '0.8'}}>
                        <strong>Légende :</strong> 
                        <span style={{marginLeft: '10px', color: '#DC2626'}}>● Violations = erreurs détectées par cette règle</span>
                    </div>
                </div>
            </div>
        </div>
    );
}