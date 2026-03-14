import { PredictionMarketsAgentClient, type AgentWsMessage } from "../src/agent-client.ts";

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

function logEvent(label: string, payload: unknown) {
  console.log(`\n[${label}]`);
  console.log(JSON.stringify(payload, null, 2));
}

async function main() {
  const client = new PredictionMarketsAgentClient({
    baseUrl: BASE_URL,
    apiKey: API_KEY,
    reconnect: true,
  });

  const [status, currentMarket, orders, fills, positions, claims] = await Promise.all([
    client.getStatus(),
    client.getCurrentMarket(),
    client.getOrders(),
    client.getFills(),
    client.getPositions(),
    client.getClaims(),
  ]);
  logEvent("status", status);
  logEvent("current market", currentMarket);
  logEvent("orders", orders);
  logEvent("fills", fills);
  logEvent("positions", positions);
  logEvent("claims", claims);

  client.onOpen = () => {
    console.log("\n[agent ws] connected");
  };
  client.onClose = () => {
    console.log("\n[agent ws] closed");
  };
  client.onError = error => {
    console.error("\n[agent ws] error", error);
  };
  client.onRecoveryRequired = payload => {
    console.log(`Recovery required for ${payload.stream}: ${payload.reason}. Rebuild local state from the next snapshot.`);
  };
  client.onMessage = (message: AgentWsMessage) => {
    logEvent(message.type, message.payload);
    if (message.type === "agent.bootstrap") {
      const marketId = message.payload.market_id || currentMarket.market_id;
      client.subscribeBook({ marketId });
      client.subscribePositions({ marketId });
      client.subscribeNonceInvalidations({ marketId });
      if (PLACE_ORDER && marketId && message.payload.current_round?.endMs) {
        client.placeOrder({
          market_id: marketId,
          side: ORDER_SIDE as "buy_yes" | "sell_yes",
          price_tenths: ORDER_PRICE_TENTHS,
          size_shares: ORDER_SIZE_SHARES,
          expiry_ms: Number(message.payload.current_round.endMs) + 30_000,
          client_order_id: `ts-demo-${Date.now()}`,
        });
      }
    }
  };

  client.connect();
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
