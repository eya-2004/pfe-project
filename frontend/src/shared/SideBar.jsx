import React, { useState } from "react";
import { NavLink, useNavigate } from "react-router-dom";
import axios from "axios";
import Icon from "./Icon";
import "../styles/SideBar.css";

export default function Sidebar({ role }) {
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const navigate = useNavigate();

  const adminLinks = [
    { to: "/admin/dashboard", icon: "chart", label: "Dashboard" },
    { to: "/admin/agents", icon: "users", label: "Gérer les agents" },
    
    { to: "/admin/history", icon: "history", label: "Consulter Historique" }, 
  ];

  const agentLinks = [
    { to: "/agent/dashboard", icon: "chart", label: "Dashboard" },
    { to: "/agent/iterationConfig",   icon: "edit",   label: "Configurer  Détection" },
    { to: "/agent/correctionConfig", icon: "settings", label: "Configurer Correction" },
    { to: "/agent/change-password", icon: "lock", label: "Changer mot de passe" },
  ];

  const links = role === "ADMIN" ? adminLinks : agentLinks;

  const handleLogout = async () => {
    try {
      await axios.post("/auth/logout", {}, { withCredentials: true });
    } catch (_) {}
    navigate("/login");
  };

  const toggleSidebar = () => {
    if (window.innerWidth <= 768) {
      setMobileOpen(!mobileOpen);
    } else {
      setCollapsed(!collapsed);
    }
  };

  return (
    <>
      <aside className={`sidebar ${collapsed ? "collapsed" : ""} ${mobileOpen ? "mobile-open" : ""}`}>
        <div className="sidebar-logo">
          <div className="sidebar-logo-icon">
            <Icon name="logo" size={20} />
          </div>
          <div className="sidebar-logo-text">
            <span className="sidebar-logo-name">DataQuality</span>
            <span className="sidebar-role">{role === "ADMIN" ? "Administrateur" : "Agent"}</span>
          </div>
          <button
            className="sidebar-collapse"
            onClick={toggleSidebar}
            aria-label={collapsed ? "Étendre" : "Réduire"}
          />
        </div>

        <nav className="sidebar-nav">
          {links.map((link) => (
            <NavLink
              key={link.to}
              to={link.to}
              className={({ isActive }) => `sidebar-link ${isActive ? "active" : ""}`}
              data-tooltip={link.label}
            >
              <span className="sidebar-link-icon">
                <Icon name={link.icon} size={18} />
              </span>
              <span className="sidebar-link-label">{link.label}</span>
            </NavLink>
          ))}
        </nav>

        <div className="sidebar-footer">
          <button className="sidebar-logout" onClick={handleLogout}>
            <span className="sidebar-logout-icon">
              <Icon name="logout" size={18} />
            </span>
            <span className="sidebar-logout-text">Déconnexion</span>
          </button>
        </div>
      </aside>

      {/* Overlay pour mobile */}
      {mobileOpen && (
        <div 
          className="sidebar-overlay" 
          onClick={() => setMobileOpen(false)}
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: 'rgba(0, 0, 0, 0.5)',
            zIndex: 999,
          }}
        />
      )}
    </>
  );
}