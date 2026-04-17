import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell,
} from "recharts";

const PURPLE = "#4f46e5";
const PURPLE_PALE = "#a5b4fc";

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const p = payload[0].payload;
  return (
    <div className="chart-tooltip">
      <p className="tt-label">{p.title}</p>
      <p className="tt-sub">{p.category} · ${Number(p.price).toFixed(2)}</p>
      <p className="tt-value">{payload[0].value.toLocaleString()} events</p>
    </div>
  );
};

export default function TopProductsBar({ products }) {
  if (!products.length)
    return <p className="empty">⏳ Waiting for Spark aggregations…</p>;

  const data = products.map((p) => ({
    ...p,
    short: p.title?.length > 32 ? p.title.slice(0, 32) + "…" : p.title,
  }));

  return (
    <ResponsiveContainer width="100%" height={360}>
      <BarChart
        data={data}
        layout="vertical"
        margin={{ top: 10, right: 30, left: 170, bottom: 10 }}
      >
        <CartesianGrid
          strokeDasharray="3 3"
          stroke="rgba(0,0,0,0.05)"
          horizontal={false}
        />
        <XAxis type="number" tick={{ fontSize: 11, fill: "#64748b" }} />
        <YAxis
          type="category"
          dataKey="short"
          width={165}
          tick={{ fontSize: 11, fill: "#475569" }}
        />
        <Tooltip content={<CustomTooltip />} cursor={{ fill: "rgba(0,0,0,0.03)" }} />
        <Bar dataKey="event_count" name="Events" radius={[0, 5, 5, 0]}>
          {data.map((_, i) => (
            <Cell
              key={i}
              fill={i === 0 ? PURPLE : PURPLE_PALE}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
