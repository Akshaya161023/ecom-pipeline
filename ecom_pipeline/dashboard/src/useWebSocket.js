import { useState, useEffect, useRef, useCallback } from "react";

const WS_URL = "ws://localhost:8080";

export default function useWebSocket() {
  const [events,     setEvents]     = useState([]);
  const [categories, setCategories] = useState([]);
  const [products,   setProducts]   = useState([]);
  const [connected,  setConnected]  = useState(false);

  const wsRef          = useRef(null);
  const reconnectTimer = useRef(null);

  const connect = useCallback(() => {
    // Clean up an existing socket before opening another one
    if (wsRef.current) {
      wsRef.current.onclose = null; // prevent auto-reconnect loop
      wsRef.current.close();
    }

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      console.log("[WS] Connected to", WS_URL);
      // Keep-alive ping every 30 s
      ws._pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send("ping");
      }, 30_000);
    };

    ws.onmessage = (e) => {
      if (e.data === "pong") return;          // ignore keep-alive reply
      const msg = JSON.parse(e.data);
      if (msg.type === "snapshot" || msg.type === "update") {
        setEvents(     msg.data.events     ?? []);
        setCategories( msg.data.categories ?? []);
        setProducts(   msg.data.products   ?? []);
      }
    };

    ws.onclose = () => {
      setConnected(false);
      clearInterval(ws._pingInterval);
      console.log("[WS] Disconnected — reconnecting in 3 s…");
      reconnectTimer.current = setTimeout(connect, 3_000);
    };

    ws.onerror = () => {
      // onclose fires right after, which handles reconnect
      ws.close();
    };
  }, []);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      clearInterval(wsRef.current?._pingInterval);
      if (wsRef.current) {
        wsRef.current.onclose = null; // don't reconnect during unmount
        wsRef.current.close();
      }
    };
  }, [connect]);

  return { events, categories, products, connected };
}
