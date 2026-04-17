export default function KpiRow({ events, categories, products }) {
  const totalEvents   = events.length;
  const totalCats     = categories.length;
  const totalProducts = products.length;

  const purchases = events.filter((e) => e.event_type === "purchase").length;
  const revenue   = events
    .filter((e) => e.event_type === "purchase")
    .reduce((sum, e) => sum + (e.price || 0), 0);

  const kpis = [
    {
      id:    "kpi-events",
      icon:  "⚡",
      label: "Total Events",
      value: totalEvents.toLocaleString(),
      color: "#4f46e5",
      glow:  "rgba(79,70,229,0.35)",
    },
    {
      id:    "kpi-categories",
      icon:  "🏷️",
      label: "Categories Active",
      value: totalCats,
      color: "#10b981",
      glow:  "rgba(16,185,129,0.35)",
    },
    {
      id:    "kpi-products",
      icon:  "📦",
      label: "Products Tracked",
      value: totalProducts,
      color: "#8b5cf6",
      glow:  "rgba(139,92,246,0.35)",
    },
    {
      id:    "kpi-purchases",
      icon:  "💳",
      label: "Purchases",
      value: purchases.toLocaleString(),
      color: "#f59e0b",
      glow:  "rgba(245,158,11,0.35)",
    },
    {
      id:    "kpi-revenue",
      icon:  "💰",
      label: "Session Revenue",
      value: `$${revenue.toLocaleString("en-US", { maximumFractionDigits: 0 })}`,
      color: "#ec4899",
      glow:  "rgba(236,72,153,0.35)",
    },
  ];

  return (
    <div className="kpi-row">
      {kpis.map(({ id, icon, label, value, color, glow }) => (
        <div
          key={id}
          id={id}
          className="kpi-card"
          style={{ "--accent": color, "--glow": glow }}
        >
          <div className="kpi-icon">{icon}</div>
          <p className="kpi-value">{value}</p>
          <p className="kpi-label">{label}</p>
          <div className="kpi-bar" />
        </div>
      ))}
    </div>
  );
}
