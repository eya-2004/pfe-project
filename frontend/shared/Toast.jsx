import React, { useEffect, useState } from 'react';
import Icon from './Icon';


export default function Toast({ toast }) {
    const [visible, setVisible] = useState(false);

    useEffect(() => {
        if (toast) {
            setVisible(true);
        } else {
            // Légère sortie animée avant de cacher
            const t = setTimeout(() => setVisible(false), 300);
            return () => clearTimeout(t);
        }
    }, [toast]);

    if (!visible && !toast) return null;

    const icons = {
        success: 'check',
        error:   'close',
        warning: 'warning',
    };

    return (
        <div className={`toast toast--${toast?.type ?? 'success'} ${toast ? 'toast--in' : 'toast--out'}`}>
            <span className="toast-icon">
                <Icon name={icons[toast?.type] ?? 'check'} size={14} />
            </span>
            <span className="toast-message">{toast?.message}</span>
        </div>
    );
}