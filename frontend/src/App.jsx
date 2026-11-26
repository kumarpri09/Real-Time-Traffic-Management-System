import { useEffect, useState } from "react";
import {
  getHealth,
  getMetrics,
  getSummary,
  getAlerts,
  getLocations
} from "./services/api";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer
} from "recharts";

import "./App.css";

function App() {
  const [health, setHealth] = useState({});
  const [metrics, setMetrics] = useState([]);
  const [summary, setSummary] = useState({});
  const [alerts, setAlerts] = useState([]);
  const [locations, setLocations] = useState([]);
  const [error, setError] = useState("");

  async function loadDashboard() {
    try {
      const results = await Promise.all([
        getHealth(),
        getMetrics(),
        getSummary(),
        getAlerts(),
        getLocations()
      ]);

      setHealth(results[0].data);
      setMetrics(results[1].data.data || []);
      setSummary(results[2].data.summary || {});
      setAlerts(results[3].data.data || []);
      setLocations(results[4].data.data || []);

      setError("");
    } catch (err) {
      setError(
        err.response?.data?.detail ||
        "Cannot connect to the backend or database."
      );
    }
  }

  useEffect(() => {
    loadDashboard();

    const interval = setInterval(loadDashboard, 5000);

    return () => clearInterval(interval);
  }, []);

  const congestionCount = (summary.total_records || 0) -
    (summary.total_records || 0);

  return (
    <div className="dashboard">

      <header className="header">
        <div>
          <h1>Smart Traffic Management</h1>
          <p>Real-Time Big Data Analytics Dashboard</p>
        </div>

        <span className="live">● LIVE MONITORING</span>
      </header>

      {error && <div className="error">{error}</div>}

      <section className="status">
        <span>API: {health.api || "checking"}</span>
        <span>PostgreSQL: {health.postgresql || "checking"}</span>
        <span>MongoDB: {health.mongodb || "checking"}</span>
      </section>

      <section className="cards">

        <div className="card">
          <h3>Total Vehicles</h3>
          <h2>{summary.total_vehicles ?? 0}</h2>
        </div>

        <div className="card">
          <h3>Average Speed</h3>
          <h2>{summary.average_speed ?? 0} km/h</h2>
        </div>

        <div className="card">
          <h3>Monitored Locations</h3>
          <h2>{summary.monitored_locations ?? 0}</h2>
        </div>

        <div className="card">
          <h3>Traffic Records</h3>
          <h2>{summary.total_records ?? 0}</h2>
        </div>

      </section>

      <section className="panel">
        <h2>Traffic Volume Analysis</h2>

        <div className="chart">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={[...metrics].reverse()}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="recorded_at" hide />
              <YAxis />
              <Tooltip />
              <Line
                type="monotone"
                dataKey="traffic_volume"
                stroke="#2563eb"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="panel">
        <h2>Location-wise Traffic</h2>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Location ID</th>
                <th>Location</th>
                <th>Average Vehicles</th>
                <th>Average Speed</th>
              </tr>
            </thead>

            <tbody>
              {locations.map((item, index) => (
                <tr key={item.location_id || index}>
                  <td>{item.location_id}</td>
                  <td>{item.location}</td>
                  <td>{Number(item.avg_traffic_volume || 0).toFixed(0)}</td>
                  <td>{Number(item.avg_speed || 0).toFixed(1)} km/h</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>Congestion Analysis</h2>

        {summary.congestion?.map((item, index) => (
          <div className="congestion-row" key={index}>
            <span>{item.congestion_status || "Unknown"}</span>
            <strong>{item.total}</strong>
          </div>
        ))}
      </section>

      <section className="panel">
        <h2>Recent Traffic Alerts</h2>

        {alerts.length === 0 && <p>No alerts available.</p>}

        {alerts.map((alert, index) => (
          <div className="alert" key={index}>
            <strong>
              {alert.location || alert.location_id || "Traffic alert"}
            </strong>
            <p>
              {alert.message || alert.alert_type || "Traffic condition detected"}
            </p>
          </div>
        ))}
      </section>

      <footer>
        Real-Time Smart Traffic Management | Big Data Analytics Project
      </footer>

    </div>
  );
}

export default App;
