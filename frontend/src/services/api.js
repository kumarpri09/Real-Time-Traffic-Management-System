import axios from "axios";

const API = axios.create({
  baseURL: import.meta.env.VITE_API_URL || "http://localhost:8000/api",
  timeout: 15000
});

export const getHealth = () => API.get("/health");

export const getMetrics = () => API.get("/metrics?limit=100");

export const getSummary = () => API.get("/summary");

export const getAlerts = () => API.get("/alerts?limit=20");

export const getLocations = () => API.get("/locations");

export default API;
