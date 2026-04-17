const COLS = ["user_id", "event_type", "category", "price", "timestamp"];

const TYPE_STYLES = {
  view:     { bg: "#e0e7ff",  color: "#3730a3", label: "view"     },
  click:    { bg: "#fef3c7",  color: "#92400e", label: "click"    },
  purchase: { bg: "#d1fae5",  color: "#065f46", label: "purchase" },
};

export default function EventsTable({ events }) {
  if (!events.length)
    return <p className="empty">⏳ Waiting for events from Kafka…</p>;

  return (
    <div className="table-wrap">
      <table className="events-table">
        <thead>
          <tr>
            {COLS.map((c) => (
              <th key={c}>{c.replace("_", " ")}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {events.map((e, i) => {
            const style = TYPE_STYLES[e.event_type] ?? {
              bg: "rgba(148,163,184,0.1)",
              color: "#94a3b8",
              label: e.event_type,
            };
            return (
              <tr key={`${e.event_id}-${i}`}>
                <td className="user-cell">{e.user_id}</td>
                <td>
                  <span
                    className="type-badge"
                    style={{ background: style.bg, color: style.color }}
                  >
                    {style.label}
                  </span>
                </td>
                <td>{e.category}</td>
                <td className="price-cell">${Number(e.price).toFixed(2)}</td>
                <td className="ts">
                  {e.timestamp?.slice(0, 19).replace("T", " ")}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
