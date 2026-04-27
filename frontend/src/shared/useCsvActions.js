import { useState, useRef } from 'react';
import axios from 'axios';

const api = axios.create({ withCredentials: true });

export function useCsvActions() {
    const [exporting, setExporting] = useState(false);
    const [importing, setImporting] = useState(false);
    const [toast, setToast]         = useState(null); // { type: 'success'|'error'|'warning', message }
    const fileInputRef              = useRef(null);

    const showToast = (type, message) => {
        setToast({ type, message });
        setTimeout(() => setToast(null), 4000);
    };

    // ── Export : erreurs PENDING pour une table spécifique ───────────────────
    const handleExport = async (tableName) => {
        if (!tableName) {
            showToast('warning', 'Veuillez sélectionner une table à exporter.');
            return;
        }

        setExporting(true);
        try {
            const response = await api.get(`/api/export/csv?status=PENDING&table=${tableName}`, {
                responseType: 'blob',
            });

            if (response.status === 204) {
                showToast('warning', `Aucune ligne avec le statut PENDING à exporter pour la table ${tableName}.`);
                return;
            }

            const date     = new Date().toISOString().slice(0, 10);
            const filename = `${tableName.toLowerCase()}_erreurs_${date}.csv`;
            const url      = window.URL.createObjectURL(new Blob([response.data]));
            const link     = document.createElement('a');
            link.href      = url;
            link.setAttribute('download', filename);
            document.body.appendChild(link);
            link.click();
            link.remove();
            window.URL.revokeObjectURL(url);

            showToast('success', `Export CSV pour la table ${tableName} téléchargé avec succès.`);
        } catch {
            showToast('error', "Erreur lors de l'export. Vérifiez la connexion.");
        } finally {
            setExporting(false);
        }
    };

    // ── Import : upload + toast uniquement ───────────────────────────────────
    const triggerFileInput = () => fileInputRef.current?.click();

    const handleFileSelected = async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;
        e.target.value = '';

        if (!file.name.endsWith('.csv')) {
            showToast('error', 'Le fichier doit être au format .csv');
            return;
        }

        setImporting(true);
        try {
            const formData = new FormData();
            formData.append('file', file);
            formData.append('correctedBy', 'agent_migration');
            formData.append('comment', `Import du ${new Date().toLocaleString('fr-FR')}`);

            const { data } = await api.post('/api/export/csv/import', formData, {
                headers: { 'Content-Type': 'multipart/form-data' },
            });

            if (data.success) {
                const n = data.updated;
                showToast('success', `Import réussi — ${n} ligne${n > 1 ? 's' : ''} corrigée${n > 1 ? 's' : ''}.`);
            } else {
                showToast('error', data.message || "L'import a échoué.");
            }
        } catch (err) {
            const msg = err.response?.data?.message || "Erreur lors de l'import.";
            showToast('error', msg);
        } finally {
            setImporting(false);
        }
    };

    return {
        exporting, handleExport,
        importing, fileInputRef, triggerFileInput, handleFileSelected,
        toast,
    };
}