import useWebSocket    from "./useWebSocket";
import KpiRow          from "./components/KpiRow";
import EventsTable     from "./components/EventsTable";
import CategoryBar     from "./components/CategoryBar";
import EventTypePie    from "./components/EventTypePie";
import TopProductsBar  from "./components/TopProductsBar";
import RevenueDonut    from "./components/RevenueDonut";
import ActivityTimeline from "./components/ActivityTimeline";
import "./styles/Dashboard.css";

export default function App() {
  const { events, categories, products, connected } = useWebSocket();

  return (
    <div className="dashboard">
      {/* ── Header ───────────────────────────────────────────── */}
      <header className="dashboard-header">
        <div className="header-left">
          <div className="logo-badge">⚡</div>
          <div>
            <h1>Real-Time E-Commerce Pipeline</h1>
            <p className="subtitle">
              🛒 Fake Store API &nbsp;→&nbsp; 📨 Kafka &nbsp;→&nbsp; ⚡ Spark &nbsp;→&nbsp; 🍃 MongoDB &nbsp;→&nbsp; 🔌 WebSocket &nbsp;→&nbsp; ⚛️ React
            </p>
          </div>
        </div>
        <div className={`status-badge ${connected ? "live" : "offline"}`}>
          <span className="dot" />
          {connected ? "Live" : "Reconnecting…"}
        </div>
      </header>

      {/* ── KPI Row ──────────────────────────────────────────── */}
      <KpiRow events={events} categories={categories} products={products} />

      {/* ── Live Events Table ────────────────────────────────── */}
      <section className="section">
        <h2>
          <span className="section-icon">📋</span>
          Live Events
          <span className="section-count">{events.length} events</span>
        </h2>
        <EventsTable events={events} />
      </section>

      {/* ── Charts Row 1 ─────────────────────────────────────── */}
      <div className="charts-row">
        <section className="chart-card">
          <h2>
            <span className="section-icon">📈</span>
            Live Activity
          </h2>
          <ActivityTimeline events={events} />
        </section>
        <section className="chart-card">
          <h2>
            <span className="section-icon">📊</span>
            Events by Category
          </h2>
          <CategoryBar categories={categories} />
        </section>
      </div>

      {/* ── Charts Row 2 ─────────────────────────────────────── */}
      <div className="charts-row">
        <section className="chart-card">
          <h2>
            <span className="section-icon">🍩</span>
            Revenue Distribution
          </h2>
          <RevenueDonut categories={categories} />
        </section>
        <section className="chart-card">
          <h2>
            <span className="section-icon">🥧</span>
            Event Action Split
          </h2>
          <EventTypePie events={events} />
        </section>
      </div>

      {/* ── Top 10 Products ──────────────────────────────────── */}
      <section className="section">
        <h2>
          <span className="section-icon">🏆</span>
          Top 10 Products
        </h2>
        <TopProductsBar products={products} />
      </section>
    </div>
  );
}
