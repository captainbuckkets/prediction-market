import { PredictionMarketsAgentClient } from "../dist/agent-client.js";

const BASE_URL = process.env.PM_BASE_URL || "http://127.0.0.1:3002/prediction-markets";
const API_KEY = process.env.PM_AGENT_API_KEY || "";
const ORDER_SIDE = process.env.PM_ORDER_SIDE || "buy_yes";
const ORDER_PRICE_TENTHS = Number(process.env.PM_ORDER_PRICE_TENTHS || 543);
const ORDER_SIZE_SHARES = Number(process.env.PM_ORDER_SIZE_SHARES || 1);
const CANCEL_AFTER_PLACE = process.env.PM_CANCEL_AFTER_PLACE !== "0";

if (!API_KEY) {
  console.error("PM_AGENT_API_KEY is required.");
  process.exit(1);
}

function logEvent(label, payload) {
  console.log(`\n[${label}]`);
  console.log(JSON.stringify(payload, null, 2));
}

async function main() {
  const client = new PredictionMarketsAgentClient({
    baseUrl: BASE_URL,
    apiKey: API_KEY,
  });

  const [me, summary, currentMarket] = await Promise.all([
    client.getMe(),
    client.getAccountSummary(),
    client.getCurrentMarket(),
  ]);
  logEvent("me", me);
  logEvent("account summary", summary);
  logEvent("current market", currentMarket);

  if (!currentMarket.market_id) {
    throw new Error("No active market.");
  }

  const placed = await client.placeOrderRest({
    market_id: currentMarket.market_id,
    side: ORDER_SIDE,
    price_tenths: ORDER_PRICE_TENTHS,
    size_shares: ORDER_SIZE_SHARES,
    client_order_id: `rest-js-${Date.now()}`,
  });
  logEvent("placed", placed);

  const fetched = await client.getOrder(placed.order.order_hash);
  logEvent("fetched", fetched);

  if (CANCEL_AFTER_PLACE) {
    const cancelled = await client.cancelOrderRest(placed.order.order_hash);
    logEvent("cancelled", cancelled);
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
