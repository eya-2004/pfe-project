import React, { useState } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import SideBar from "../shared/SideBar";
import DetectedInconsistency from "../shared/DetectedInconsistency";
import ChangePassword from "./ChangePassword";
import CorrectionConfig from "./CorrectionConfig";

export default function AgentDashboard() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  return (
    <div className="dashboard-shell">
      <SideBar role="AGENT" onCollapse={setSidebarCollapsed} />
      <main className={`dashboard-main${sidebarCollapsed ? " expanded" : ""}`}>
        <Routes>
          <Route path="/" element={<Navigate to="dashboard" replace />} />
          <Route path="dashboard" element={<DetectedInconsistency role="agent" />} />
          <Route path="inconsistencies" element={<DetectedInconsistency role="agent" />} />
          <Route path="iterationConfig" element={<DetectedInconsistency role="agent" />} />
          <Route path="correctionConfig" element={<CorrectionConfig />} />
          <Route path="change-password" element={<ChangePassword />} />
        </Routes>
      </main>
    </div>
  );
}