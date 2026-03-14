import { PredictionMarketsAgentClient, type AgentWsMessage } from "../src/agent-client.ts";

const BASE_URL = process.env.PM_BASE_URL || "http://127.0.0.1:3002/prediction-markets";
const API_KEY = process.env.PM_AGENT_API_KEY || "";
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

  const currentMarket = await client.getCurrentMarket();
  logEvent("current market", currentMarket);

  let placedOrderHash: string | null = null;

  client.onMessage = async (message: AgentWsMessage) => {
    logEvent(message.type, message.payload);

    if (message.type === "agent.bootstrap") {
      const marketId = message.payload.market_id || currentMarket.market_id;
      client.subscribeBook({ marketId });
      client.subscribePositions({ marketId });
      client.subscribeNonceInvalidations({ marketId });
      if (marketId && !placedOrderHash) {
        const placed = await client.placeOrderRest({
          market_id: marketId,
          side: ORDER_SIDE as "buy_yes" | "sell_yes",
          price_tenths: ORDER_PRICE_TENTHS,
          size_shares: ORDER_SIZE_SHARES,
          client_order_id: `hybrid-ts-${Date.now()}`,
        });
        placedOrderHash = placed.order.order_hash;
        logEvent("rest placed", placed);
      }
      return;
    }

    if (message.type === "order.accepted" && placedOrderHash && message.payload?.order?.order_hash === placedOrderHash) {
      const cancelled = await client.cancelOrderRest(placedOrderHash);
      logEvent("rest cancelled", cancelled);
      return;
    }

    if (message.type === "order.cancelled" && placedOrderHash && message.payload?.order_hash === placedOrderHash) {
      process.exit(0);
    }
  };

  client.connect();
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
