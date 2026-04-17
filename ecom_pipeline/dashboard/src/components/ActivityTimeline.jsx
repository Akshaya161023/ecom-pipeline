import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer,
} from "recharts";

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <p className="tt-label">Time: {label}</p>
      <p className="tt-value" style={{ color: "#10b981" }}>
        {payload[0].value} events/sec
      </p>
    </div>
  );
};

export default function ActivityTimeline({ events }) {
  if (!events.length)
    return <p className="empty">⏳ Waiting for events…</p>;

  // Group events by second (HH:MM:SS)
  const counts = events.reduce((acc, e) => {
    const time = e.timestamp ? e.timestamp.slice(11, 19) : "00:00:00";
    acc[time] = (acc[time] || 0) + 1;
    return acc;
  }, {});

  const data = Object.entries(counts)
    .sort(([a], [b]) => a.localeCompare(b))
    .slice(-30) // Show last 30 active seconds
    .map(([time, count]) => ({ time, count }));

  return (
    <ResponsiveContainer width="100%" height={280}>
      <AreaChart data={data} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
        <defs>
          <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.05)" vertical={false} />
        <XAxis 
          dataKey="time" 
          tick={{ fontSize: 11, fill: "#64748b" }} 
          minTickGap={20}
        />
        <YAxis tick={{ fontSize: 11, fill: "#64748b" }} />
        <Tooltip content={<CustomTooltip />} cursor={{ stroke: "rgba(0,0,0,0.05)", strokeWidth: 2 }} />
        <Area
          type="monotone"
          dataKey="count"
          stroke="#10b981"
          strokeWidth={3}
          fillOpacity={1}
          fill="url(#colorCount)"
          animationDuration={300}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
