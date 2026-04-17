import {
  PieChart, Pie, Cell, Tooltip,
  Legend, ResponsiveContainer,
} from "recharts";

const COLORS = {
  view:     "#6366f1",
  click:    "#f59e0b",
  purchase: "#10b981",
};

const RADIAN = Math.PI / 180;

const renderLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent }) => {
  if (percent < 0.05) return null;
  const r  = innerRadius + (outerRadius - innerRadius) * 0.55;
  const x  = cx + r * Math.cos(-midAngle * RADIAN);
  const y  = cy + r * Math.sin(-midAngle * RADIAN);
  return (
    <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central"
      fontSize={12} fontWeight={600}>
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <p className="tt-label">{payload[0].name}</p>
      <p className="tt-value">{payload[0].value.toLocaleString()} events</p>
    </div>
  );
};

export default function EventTypePie({ events }) {
  if (!events.length)
    return <p className="empty">⏳ Waiting for events…</p>;

  const counts = events.reduce((acc, e) => {
    acc[e.event_type] = (acc[e.event_type] || 0) + 1;
    return acc;
  }, {});

  const data = Object.entries(counts).map(([name, value]) => ({ name, value }));

  return (
    <ResponsiveContainer width="100%" height={280}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="44%"
          outerRadius={95}
          dataKey="value"
          nameKey="name"
          labelLine={false}
          label={renderLabel}
        >
          {data.map((d) => (
            <Cell key={d.name} fill={COLORS[d.name] ?? "#64748b"} />
          ))}
        </Pie>
        <Tooltip content={<CustomTooltip />} />
        <Legend
          iconType="circle"
          iconSize={8}
          wrapperStyle={{ fontSize: 12, color: "#64748b" }}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}
