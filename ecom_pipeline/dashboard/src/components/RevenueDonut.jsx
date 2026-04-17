import {
  PieChart, Pie, Cell, Tooltip,
  Legend, ResponsiveContainer,
} from "recharts";

const PALETTE = [
  "#ec4899", "#8b5cf6", "#3b82f6", 
  "#10b981", "#f59e0b", "#ef4444"
];

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <p className="tt-label">{payload[0].name}</p>
      <p className="tt-value">
        ${payload[0].value.toLocaleString("en-US", { maximumFractionDigits: 2 })}
      </p>
    </div>
  );
};

export default function RevenueDonut({ categories }) {
  if (!categories.length)
    return <p className="empty">⏳ Waiting for Spark aggregations…</p>;

  // Compute revenue per category
  const data = categories
    .map((c) => ({
      name: c.category,
      value: c.purchases * (c.avg_price || 0),
    }))
    .filter((d) => d.value > 0)
    .sort((a, b) => b.value - a.value);

  if (!data.length)
    return <p className="empty">⏳ Waiting for first purchase…</p>;

  return (
    <ResponsiveContainer width="100%" height={280}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="45%"
          innerRadius={65}
          outerRadius={95}
          paddingAngle={4}
          dataKey="value"
          nameKey="name"
          stroke="none"
        >
          {data.map((entry, index) => (
            <Cell key={entry.name} fill={PALETTE[index % PALETTE.length]} />
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
