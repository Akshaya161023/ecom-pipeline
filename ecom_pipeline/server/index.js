const express             = require("express");
const cors                = require("cors");
const http                = require("http");
const { WebSocketServer } = require("ws");
const { connect, getSnapshot, watchCollections } = require("./db");
const statsRouter         = require("./routes/stats");

const PORT = process.env.PORT || 3001;

const app    = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ── Middleware ─────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());
app.use("/api", statsRouter);

app.get("/health", (_req, res) => res.json({ status: "ok" }));

// ── WebSocket ──────────────────────────────────────────────────────────────

/** Send a JSON payload to every open WS client. */
function broadcast(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1 /* WebSocket.OPEN */) {
      client.send(msg);
    }
  });
}

wss.on("connection", async (ws) => {
  console.log(`[WS] Client connected — ${wss.clients.size} total`);

  // ── Send full snapshot immediately so the dashboard has data right away ──
  try {
    const data = await getSnapshot();
    ws.send(
      JSON.stringify({ type: "snapshot", data, timestamp: new Date().toISOString() })
    );
  } catch (err) {
    console.error("[WS] Snapshot error on connect:", err.message);
  }

  // ── Keep-alive ping / pong ────────────────────────────────────────────────
  ws.on("message", (raw) => {
    if (raw.toString() === "ping") ws.send("pong");
  });

  ws.on("close", () => {
    console.log(`[WS] Client disconnected — ${wss.clients.size} remaining`);
  });

  ws.on("error", (err) => {
    console.error("[WS] Client error:", err.message);
  });
});

// ── Bootstrap ──────────────────────────────────────────────────────────────
async function main() {
  try {
    await connect();
  } catch (err) {
    console.log("DB connection skipped:", err.message);
  }
  // Watch MongoDB change streams → broadcast fresh data to all WS clients
  try {
  await watchCollections(async () => {
      });
} catch (err) {
  console.log("Watch skipped:", err.message);
}
    try {
      const data = await getSnapshot();
      broadcast({ type: "update", data, timestamp: new Date().toISOString() });
      console.log(`[WS] Broadcasted update to ${wss.clients.size} client(s)`);
    } catch (err) {
      console.error("[WS] Broadcast error:", err.message);
    }
  });

  server.listen(PORT, () => {
    console.log(`[Server] Running on port ${PORT}`);
    console.log(`[WebSocket] Shared same port`);
  });
}

main().catch((err) => {
  console.error("[Server] Fatal error:", err);
  process.exit(1);
});
