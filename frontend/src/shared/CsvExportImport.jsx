import React, { useState } from 'react';
import { useCsvActions } from './useCsvActions.js';
import Icon from './Icon';
import Toast from './Toast.jsx';

export default function CsvExportImport({ tables, selectedTable, onTableChange }) {
    const [showDropdown, setShowDropdown] = useState(false);
    const {
        exporting, handleExport,
        importing, fileInputRef, triggerFileInput, handleFileSelected,
        toast,
    } = useCsvActions();

    const handleTableChange = (e) => {
        onTableChange(e.target.value);
        setShowDropdown(false);
    };

    return (
        <div className="csv-actions-container">
            <div className="table-selector">
                <label htmlFor="csv-table-select">Table pour CSV</label>
                <div className="dropdown-container">
                    <button
                        className="dropdown-button"
                        onClick={() => setShowDropdown(!showDropdown)}
                        disabled={tables.length === 0}
                    >
                        {selectedTable || "Sélectionner une table"}
                        <Icon name="chevron-down" size={14} />
                    </button>
                    {showDropdown && (
                        <div className="dropdown-menu">
                            {tables.map(table => (
                                <button
                                    key={table}
                                    className={`dropdown-item ${selectedTable === table ? 'active' : ''}`}
                                    onClick={() => handleTableChange({ target: { value: table } })}
                                >
                                    {table}
                                </button>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            <div className="action-buttons">
                <button
                    className={`export-button ${exporting ? 'loading' : ''}`}
                    onClick={() => handleExport(selectedTable)}
                    disabled={!selectedTable || exporting || importing}
                >
                    {exporting ? (
                        <>
                            <div className="spinner"></div>
                            Exportation...
                        </>
                    ) : (
                        <>
                            <Icon name="download" size={16} />
                            Exporter CSV
                        </>
                    )}
                </button>

                <button
                    className={`import-button ${importing ? 'loading' : ''}`}
                    onClick={triggerFileInput}
                    disabled={!selectedTable || exporting || importing}
                >
                    {importing ? (
                        <>
                            <div className="spinner"></div>
                            Importation...
                        </>
                    ) : (
                        <>
                            <Icon name="upload" size={16} />
                            Importer CSV
                        </>
                    )}
                </button>

                <input
                    type="file"
                    accept=".csv"
                    ref={fileInputRef}
                    onChange={handleFileSelected}
                    style={{ display: 'none' }}
                />
            </div>

            <Toast toast={toast} />
        </div>
    );
}