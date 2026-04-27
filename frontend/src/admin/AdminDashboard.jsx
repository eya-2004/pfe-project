// src/admin/AdminDashboard.jsx
import React, { useState } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import SideBar from "../shared/SideBar";
import DetectedInconsistency from "../shared/DetectedInconsistency";
import ManageAgents from "./ManageAgents";
import AgentIterationHistory from "./AgentIterationHistory";
export default function AdminDashboard() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  return (
    <div className="dashboard-shell">
      <SideBar role="ADMIN" onCollapse={setSidebarCollapsed} />
      <main className={`dashboard-main${sidebarCollapsed ? " expanded" : ""}`}>
        <Routes>
          <Route path="/"                element={<Navigate to="dashboard" replace />} />
          <Route path="dashboard"        element={<DetectedInconsistency role="admin" />} />
          <Route path="agents"           element={<ManageAgents />} />
          <Route path="inconsistencies"  element={<DetectedInconsistency role="admin" />} />
          <Route path="history" element={<AgentIterationHistory />} />
        </Routes>
      </main>
    </div>
  );
}