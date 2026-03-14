import { privateKeyToAccount } from "viem/accounts";
import { PredictionMarketsAgentClient } from "../src/agent-client.ts";
import { buildPredictionMarketOrderTypedData, normalizePredictionMarketOrderMessage } from "../src/order-signing.js";

const BASE_URL = process.env.PM_BASE_URL || "http://127.0.0.1:3002/prediction-markets";
const API_KEY = process.env.PM_AGENT_API_KEY || "";
const MAKER_PRIVATE_KEY = process.env.PM_MAKER_PRIVATE_KEY || "";
const ORDER_SIDE = process.env.PM_ORDER_SIDE || "buy_yes";
const ORDER_PRICE_TENTHS = Number(process.env.PM_ORDER_PRICE_TENTHS || 543);
const ORDER_SIZE_SHARES = Number(process.env.PM_ORDER_SIZE_SHARES || 1);

if (!API_KEY) {
  console.error("PM_AGENT_API_KEY is required.");
  process.exit(1);
}

if (!MAKER_PRIVATE_KEY) {
  console.error("PM_MAKER_PRIVATE_KEY is required.");
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
  });
  const [protocol, currentMarket] = await Promise.all([client.getProtocol(), client.getCurrentMarket()]);
  if (!currentMarket.market_id || !currentMarket.current_round?.endMs) {
    throw new Error("No active market.");
  }

  const account = privateKeyToAccount(MAKER_PRIVATE_KEY as `0x${string}`);
  const expiryMs = Number(currentMarket.current_round.endMs) + 30_000;
  const nonce = Date.now();
  const message = normalizePredictionMarketOrderMessage({
    market_id: currentMarket.market_id,
    maker_address: account.address,
    side: ORDER_SIDE,
    price_tenths: ORDER_PRICE_TENTHS,
    size_shares: ORDER_SIZE_SHARES,
    nonce,
    expiry_ms: expiryMs,
  });
  const typedData = buildPredictionMarketOrderTypedData({
    message,
    chainId: Number(protocol.signing.chain_id),
    verifyingContract: protocol.signing.verifying_contract,
  });
  const signature = await account.signTypedData(typedData);
  const placed = await client.placeOrderRest({
    market_id: currentMarket.market_id,
    maker_address: account.address,
    side: ORDER_SIDE as "buy_yes" | "sell_yes",
    price_tenths: ORDER_PRICE_TENTHS,
    size_shares: ORDER_SIZE_SHARES,
    nonce,
    expiry_ms: expiryMs,
    signature,
    client_order_id: `signed-ts-${Date.now()}`,
  });

  logEvent("protocol signing config", protocol.signing);
  logEvent("signed order", {
    message,
    signature,
  });
  logEvent("placed", placed);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
