import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell,
} from "recharts";

const PALETTE = [
  "#6366f1", "#10b981", "#f59e0b",
  "#ef4444", "#8b5cf6", "#ec4899",
];

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <p className="tt-label">{label}</p>
      <p className="tt-value">{payload[0].value.toLocaleString()} events</p>
    </div>
  );
};

export default function CategoryBar({ categories }) {
  if (!categories.length)
    return <p className="empty">⏳ Waiting for Spark aggregations…</p>;

  const data = [...categories].sort((a, b) => b.event_count - a.event_count);

  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.05)" vertical={false} />
        <XAxis
          dataKey="category"
          angle={-30}
          textAnchor="end"
          tick={{ fontSize: 11, fill: "#64748b" }}
        />
        <YAxis tick={{ fontSize: 11, fill: "#64748b", dx: -5 }} />
        <Tooltip content={<CustomTooltip />} cursor={{ fill: "rgba(0,0,0,0.03)" }} />
        <Bar dataKey="event_count" name="Events" radius={[5, 5, 0, 0]}>
          {data.map((_, i) => (
            <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
