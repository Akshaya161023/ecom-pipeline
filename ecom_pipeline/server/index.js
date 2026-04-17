const express = require("express");
const cors = require("cors");
const http = require("http");
const { WebSocketServer } = require("ws");
const path = require("path");
const { connect, getSnapshot, watchCollections } = require("./db");
const statsRouter = require("./routes/stats");

const PORT = process.env.PORT || 3001;

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

/* -------------------------------------------------------------------------- */
/* Middleware                                                                 */
/* -------------------------------------------------------------------------- */

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));
app.use("/api", statsRouter);

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/health", (_req, res) => res.json({ status: "ok" }));

/* -------------------------------------------------------------------------- */
/* WebSocket Broadcast Helper                                                 */
/* -------------------------------------------------------------------------- */

function broadcast(payload) {
  const message = JSON.stringify(payload);

  wss.clients.forEach((client) => {
    if (client.readyState === 1) {
      client.send(message);
    }
  });
}

/* -------------------------------------------------------------------------- */
/* WebSocket Connections                                                      */
/* -------------------------------------------------------------------------- */

wss.on("connection", async (ws) => {
  console.log(`[WS] Client connected - ${wss.clients.size} total`);

  // Send initial snapshot
  try {
    const data = await getSnapshot();

    ws.send(
      JSON.stringify({
        type: "snapshot",
        data,
        timestamp: new Date().toISOString(),
      })
    );
  } catch (err) {
    console.error("[WS] Snapshot error:", err.message);
  }

  // Ping / Pong
  ws.on("message", (raw) => {
    if (raw.toString() === "ping") {
      ws.send("pong");
    }
  });

  ws.on("close", () => {
    console.log(`[WS] Client disconnected - ${wss.clients.size} remaining`);
  });

  ws.on("error", (err) => {
    console.error("[WS] Client error:", err.message);
  });
});

/* -------------------------------------------------------------------------- */
/* Main Bootstrap                                                             */
/* -------------------------------------------------------------------------- */

async function main() {
  // Connect DB
  try {
    await connect();
    console.log("[DB] Connected");
  } catch (err) {
    console.log("[DB] Connection skipped:", err.message);
  }

  // Watch DB changes
  try {
    await watchCollections(async () => {
      try {
        const data = await getSnapshot();

        broadcast({
          type: "update",
          data,
          timestamp: new Date().toISOString(),
        });

        console.log(
          `[WS] Broadcasted update to ${wss.clients.size} client(s)`
        );
      } catch (err) {
        console.error("[WS] Broadcast error:", err.message);
      }
    });
  } catch (err) {
    console.log("[DB] Watch skipped:", err.message);
  }

  // Start Server
  server.listen(PORT, () => {
    console.log(`[Server] Running on port ${PORT}`);
    console.log("[WebSocket] Running on same port");
  });
}

/* -------------------------------------------------------------------------- */
/* Start App                                                                  */
/* -------------------------------------------------------------------------- */

main().catch((err) => {
  console.error("[Server] Fatal error:", err);
  process.exit(1);
});
app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});