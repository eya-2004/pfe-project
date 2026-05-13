import React from "react";
import './App.css';

import './styles/Sidebar.css';

import './shared/dashboard.css';
import './admin/manageAgents.css'
import './auth/auth.css';
import IterationConfig from "./agent/IterationConfig";
import './shared/toast.css';
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Login          from "./auth/Login";
import Register       from "./auth/Register";
import AdminDashboard from "./admin/AdminDashboard";
import AgentDashboard from "./agent/AgentDashboard";
import ChangePassword from "./agent/ChangePassword";

export default  function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login"           element={<Login />} />
        <Route path="/register"        element={<Register />} />
        <Route path="/change-password"element={<ChangePassword />} />  {/* for first login */}
        <Route path="/agent/change-password"  element={<ChangePassword />} />  {/* for sidebar link */}
        <Route path="/admin/*"         element={<AdminDashboard />} />
        <Route path="/agent/*"         element={<AgentDashboard />} />
        <Route path="/agent/iterationConfig" element={<IterationConfig />} />
        <Route path="/"                element={<Navigate to="/login" replace />} />
        <Route path="*"                element={<Navigate to="/login" replace />} />
      </Routes>
    </BrowserRouter>
  );
}