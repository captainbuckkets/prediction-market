const BASE_URL = process.env.PM_BASE_URL || "http://127.0.0.1:3002/prediction-markets";
const API_KEY = process.env.PM_AGENT_API_KEY || "";
const PLACE_ORDER = process.env.PM_PLACE_ORDER === "1";
const ORDER_SIDE = process.env.PM_ORDER_SIDE || "buy_yes";
const ORDER_PRICE_TENTHS = Number(process.env.PM_ORDER_PRICE_TENTHS || 543);
const ORDER_SIZE_SHARES = Number(process.env.PM_ORDER_SIZE_SHARES || 1);

if (!API_KEY) {
  console.error("PM_AGENT_API_KEY is required.");
  process.exit(1);
}

function authHeaders() {
  return {
    Authorization: `Bearer ${API_KEY}`,
    "Content-Type": "application/json",
  };
}

async function getJson(path) {
  const response = await fetch(`${BASE_URL}${path}`, {
    headers: authHeaders(),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `${path} failed with ${response.status}`);
  }
  return payload;
}

async function postJson(path, body) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(body),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `${path} failed with ${response.status}`);
  }
  return payload;
}

function buildAgentWsUrl() {
  const wsBase = BASE_URL.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
  const url = new URL(`${wsBase}/ws/agent`);
  url.searchParams.set("api_key", API_KEY);
  return url.toString();
}

function logEvent(label, payload) {
  console.log(`\n[${label}]`);
  console.log(JSON.stringify(payload, null, 2));
}

async function bootstrapRest() {
  const [status, currentMarket, orders, fills, positions, claims] = await Promise.all([
    getJson("/api/status"),
    getJson("/api/markets/current"),
    getJson("/api/orders"),
    getJson("/api/fills"),
    getJson("/api/positions"),
    getJson("/api/claims"),
  ]);
  logEvent("status", status);
  logEvent("current market", currentMarket);
  logEvent("orders", orders);
  logEvent("fills", fills);
  logEvent("positions", positions);
  logEvent("claims", claims);
  return currentMarket;
}

async function main() {
  const currentMarket = await bootstrapRest();
  const socket = new WebSocket(buildAgentWsUrl());

  socket.addEventListener("open", () => {
    console.log("\n[agent ws] connected");
  });

  socket.addEventListener("message", async event => {
    const message = JSON.parse(typeof event.data === "string" ? event.data : String(event.data));
    logEvent(message.type, message.payload);

    if (message.type === "agent.bootstrap") {
      const marketId = message.payload.market_id || currentMarket.market_id;
      socket.send(JSON.stringify({ type: "book.subscribe", payload: { market_id: marketId } }));
      socket.send(JSON.stringify({ type: "positions.subscribe", payload: { market_id: marketId } }));
      socket.send(JSON.stringify({ type: "nonce_invalidations.subscribe", payload: { market_id: marketId } }));

      if (PLACE_ORDER && marketId && message.payload.current_round?.endMs) {
        const expiryMs = Number(message.payload.current_round.endMs) + 30_000;
        socket.send(
          JSON.stringify({
            type: "order.place",
            payload: {
              market_id: marketId,
              side: ORDER_SIDE,
              price_tenths: ORDER_PRICE_TENTHS,
              size_shares: ORDER_SIZE_SHARES,
              expiry_ms: expiryMs,
              client_order_id: `js-demo-${Date.now()}`,
            },
          }),
        );
      }
      return;
    }

    if (message.type === "stream.recovery_required") {
      console.log("Recovery required. Resubscribe without since_sequence or rebuild local state from the snapshot.");
    }
  });

  socket.addEventListener("close", () => {
    console.log("\n[agent ws] closed");
  });

  socket.addEventListener("error", error => {
    console.error("\n[agent ws] error", error);
  });
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
