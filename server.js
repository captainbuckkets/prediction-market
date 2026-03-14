import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join, normalize, extname } from "node:path";
import { Pool } from "pg";
import {
  createPublicClient,
  createWalletClient,
  getAddress,
  http,
  isAddress,
  keccak256,
  stringToHex,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { abstractTestnet } from "viem/chains";
import { predictionMarketFillManagerAbi } from "./src/onchain/prediction-market-fill-manager-abi.js";
import { predictionMarketNonceManagerAbi } from "./src/onchain/prediction-market-nonce-manager-abi.js";
import { predictionMarketVaultAbi } from "./src/onchain/prediction-market-vault-abi.js";
import { predictionMarketClearinghouseAbi } from "./src/onchain/prediction-market-clearinghouse-abi.js";
import { predictionMarketSettlementManagerAbi } from "./src/onchain/prediction-market-settlement-manager-abi.js";
import { buildMonitoringSnapshot, evaluateMonitoringAlerts } from "./src/monitoring.js";
import {
  ORDERBOOK_DOMAIN_NAME,
  ORDERBOOK_DOMAIN_VERSION,
  ORDERBOOK_PRIMARY_TYPE,
  ORDERBOOK_DEFAULT_CHAIN_ID,
  ORDERBOOK_DEFAULT_VERIFYING_CONTRACT,
  hashPredictionMarketOrder,
  microsharesToShares,
  normalizePredictionMarketOrderMessage,
  recoverPredictionMarketOrderSigner,
  sharesToMicroshares,
} from "./src/order-signing.js";

const ROOT_DIR = import.meta.dir;
const PUBLIC_DIR = join(ROOT_DIR, "public");
const CLI_ARGS = process.argv.slice(2);
const hasCliFlag = flag => CLI_ARGS.includes(flag);
const readCliFlag = flag => {
  const index = CLI_ARGS.indexOf(flag);
  if (index === -1) return null;
  return CLI_ARGS[index + 1] ?? null;
};
const parseBlockArg = value => {
  if (value === null || value === undefined || value === "" || value === "latest") return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
};
const EXPLICIT_CONFIG_PATH = readCliFlag("--config");
const RECONCILE_ONCE = hasCliFlag("--reconcile-once");
const RECONCILE_UPDATE_CURSORS = hasCliFlag("--update-cursors");
const RECONCILE_FROM_BLOCK = parseBlockArg(readCliFlag("--from-block"));
const RECONCILE_TO_BLOCK = parseBlockArg(readCliFlag("--to-block"));
const DEFAULT_CONFIG = {
  port: 3002,
  basePath: "/prediction-markets",
  binanceFuturesStreamUrl: "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker",
  symbol: "BTCUSDT",
  databaseUrl: null,
  agentApiKeys: [],
  disableUpstreamFeed: false,
  disableLedgerHydration: false,
  disableSecondTimer: false,
  nonceRelayMode: "disabled",
  fillSettlementMode: "offchain",
  settlementMode: "offchain",
  nonceRelayPollMs: 500,
  reconciliationPollMs: 5000,
  monitoringPollMs: 10000,
  wsReplayHistoryLimit: 256,
  streamBackfillRetentionLimit: 4096,
  monitoringWebhookUrl: null,
  monitoringThresholds: null,
  abstractTestnetRpcUrl: null,
  nonceManagerAddress: null,
  fillManagerAddress: null,
  settlementManagerAddress: null,
  vaultAddress: null,
  clearinghouseAddress: null,
  nonceRelayPrivateKey: null,
  orderSigningChainId: ORDERBOOK_DEFAULT_CHAIN_ID,
  orderSigningVerifyingContract: ORDERBOOK_DEFAULT_VERIFYING_CONTRACT,
  initialLastPrice: null,
  initialQuote: null,
  initialTrade: null,
  initialRound: null,
};
const loadLocalConfig = () => {
  const configCandidates = [
    EXPLICIT_CONFIG_PATH,
    join(ROOT_DIR, "config.local.json"),
    join(ROOT_DIR, "config.json"),
  ].filter(Boolean);
  for (const candidate of configCandidates) {
    if (!existsSync(candidate)) continue;
    try {
      const parsed = JSON.parse(readFileSync(candidate, "utf8"));
      return { ...DEFAULT_CONFIG, ...parsed, __source: candidate };
    } catch (error) {
      console.error(`[prediction-markets] failed to parse config file ${candidate}`, error);
      break;
    }
  }
  return { ...DEFAULT_CONFIG, __source: null };
};

const APP_CONFIG = loadLocalConfig();
const PORT = Number(APP_CONFIG.port) || DEFAULT_CONFIG.port;
const BASE_PATH = APP_CONFIG.basePath;
const BINANCE_STREAM_URL = APP_CONFIG.binanceFuturesStreamUrl;
const SYMBOL = APP_CONFIG.symbol;
const DISABLE_UPSTREAM_FEED = Boolean(APP_CONFIG.disableUpstreamFeed);
const DISABLE_LEDGER_HYDRATION = Boolean(APP_CONFIG.disableLedgerHydration);
const DISABLE_SECOND_TIMER = Boolean(APP_CONFIG.disableSecondTimer);
const NONCE_RELAY_MODE = APP_CONFIG.nonceRelayMode ?? "disabled";
const FILL_SETTLEMENT_MODE = APP_CONFIG.fillSettlementMode ?? "offchain";
const SETTLEMENT_MODE = APP_CONFIG.settlementMode ?? "offchain";
const NONCE_RELAY_POLL_MS = Math.max(100, Number(APP_CONFIG.nonceRelayPollMs) || 500);
const RECONCILIATION_POLL_MS = Math.max(1000, Number(APP_CONFIG.reconciliationPollMs) || 5000);
const MONITORING_POLL_MS = Math.max(1000, Number(APP_CONFIG.monitoringPollMs) || 10000);
const WS_REPLAY_HISTORY_LIMIT = Math.max(1, Number(APP_CONFIG.wsReplayHistoryLimit) || 256);
const STREAM_BACKFILL_RETENTION_LIMIT = Math.max(
  WS_REPLAY_HISTORY_LIMIT,
  Number(APP_CONFIG.streamBackfillRetentionLimit) || 4096,
);
const MONITORING_WEBHOOK_URL = APP_CONFIG.monitoringWebhookUrl ?? null;
const MONITORING_THRESHOLDS = APP_CONFIG.monitoringThresholds ?? {};
const ABSTRACT_TESTNET_RPC_URL = APP_CONFIG.abstractTestnetRpcUrl ?? null;
const NONCE_MANAGER_ADDRESS = APP_CONFIG.nonceManagerAddress ?? null;
const FILL_MANAGER_ADDRESS = APP_CONFIG.fillManagerAddress ?? null;
const SETTLEMENT_MANAGER_ADDRESS = APP_CONFIG.settlementManagerAddress ?? null;
const VAULT_ADDRESS = APP_CONFIG.vaultAddress ?? null;
const CLEARINGHOUSE_ADDRESS = APP_CONFIG.clearinghouseAddress ?? null;
const NONCE_RELAY_PRIVATE_KEY = APP_CONFIG.nonceRelayPrivateKey ?? null;
const ORDER_SIGNING_CHAIN_ID = Number(APP_CONFIG.orderSigningChainId) || ORDERBOOK_DEFAULT_CHAIN_ID;
const HAS_EXPLICIT_ORDER_SIGNING_VERIFYING_CONTRACT =
  typeof APP_CONFIG.orderSigningVerifyingContract === "string" && APP_CONFIG.orderSigningVerifyingContract.length > 0;
const ORDER_SIGNING_VERIFYING_CONTRACT =
  HAS_EXPLICIT_ORDER_SIGNING_VERIFYING_CONTRACT
    ? APP_CONFIG.orderSigningVerifyingContract
    : FILL_MANAGER_ADDRESS ?? ORDERBOOK_DEFAULT_VERIFYING_CONTRACT;
const NONCE_MANAGER_ABI = predictionMarketNonceManagerAbi;
const FILL_MANAGER_ABI = predictionMarketFillManagerAbi;
const VAULT_ABI = predictionMarketVaultAbi;
const CLEARINGHOUSE_ABI = predictionMarketClearinghouseAbi;
const SETTLEMENT_MANAGER_ABI = predictionMarketSettlementManagerAbi;
const ROUND_MS = 5 * 60 * 1000;
const CHART_WINDOW_MS = 15 * 60 * 1000;
const LEDGER_DATABASE_URL = APP_CONFIG.databaseUrl ?? null;
const LEDGER_VENUE = "binance-futures";
const MAX_RESOLVED_ROWS = 288;
const RESOLUTION_PAGE_SIZE_DEFAULT = 12;
const RESOLUTION_PAGE_SIZE_MAX = 50;
const ORDER_PAGE_SIZE_DEFAULT = 50;
const ORDER_PAGE_SIZE_MAX = 200;
const FILL_PAGE_SIZE_DEFAULT = 50;
const FILL_PAGE_SIZE_MAX = 200;
const PRICE_TENTHS_MIN = 1;
const PRICE_TENTHS_MAX = 999;
const DEFAULT_ORDER_EXPIRY_BUFFER_MS = 30 * 1000;
const DEFAULT_OPEN_ORDER_STATUSES = new Set(["open", "partially_filled", "cancel_pending"]);
const AUTH_CACHE_TTL_MS = 30 * 1000;
const AUTH_LAST_USED_WRITE_TTL_MS = 60 * 1000;
const DEFAULT_AGENT_SCOPES = [
  "market:read",
  "orders:write",
  "orders:cancel",
  "fills:take",
  "claims:write",
  "nonce_invalidations:read",
];
const AGENT_NOTIFICATION_EVENT_TYPES = [
  "order.accepted",
  "order.cancelled",
  "fill.executed",
  "claim.accepted",
];
const AGENT_PROTOCOL_VERSION = "2026-03-13";
const AGENT_PROTOCOL_SCHEMA_PATH = `${BASE_PATH}/api/agent/protocol`;
const AGENT_PROTOCOL_SCHEMAS_PATH = `${BASE_PATH}/api/agent/schemas`;
const AGENT_IDEMPOTENCY = {
  order_place: {
    supports_client_order_id: true,
    client_order_id_unique: false,
    safe_retry_without_signature: false,
    safe_retry_with_same_signature: true,
    notes: [
      "Unsigned order.place is not idempotent by client_order_id. Repeating the same payload creates another order.",
      "Signed order.place is effectively idempotent by signed order hash because the server rejects duplicate order_hash values.",
    ],
  },
  order_cancel: {
    idempotent_terminal_state: true,
    notes: [
      "A repeated cancel after cancellation will reject because the order is no longer open.",
      "Clients should treat order.cancel as terminal once order.cancelled or a non-open status is observed.",
    ],
  },
  claim_submit: {
    idempotent_terminal_state: true,
    notes: [
      "Repeated claim submission is safe only after checking current claims and positions.",
      "A duplicate submitted claim returns a rejection once no claimable balance remains or the claim already exists.",
    ],
  },
};
const AGENT_RATE_LIMIT_POLICY = {
  algorithm: "fixed_window",
  window_ms: 60_000,
  key_dimension: "key_id",
  over_limit_status: 429,
  retry_signal: "error message includes retry-after seconds",
};
const AGENT_ERROR_CODES = {
  AUTH_REQUIRED: { retryable: false, http_status: 401, message: "Authentication required." },
  AUTH_INVALID_API_KEY: { retryable: false, http_status: 401, message: "Invalid or missing agent API key." },
  AUTH_SCOPE_MISSING: { retryable: false, http_status: 403, message: "Agent key is missing the required scope." },
  RATE_LIMIT_EXCEEDED: { retryable: true, http_status: 429, message: "Rate limit exceeded for agent key." },
  LEDGER_UNAVAILABLE: { retryable: true, http_status: 503, message: "Ledger is not configured." },
  MARKET_NOT_ACTIVE: { retryable: true, http_status: 400, message: "No active market." },
  MARKET_NOT_LIVE: { retryable: true, http_status: 400, message: "Market is not open for trading." },
  MARKET_NOT_LIVE_FOR_FILLS: { retryable: true, http_status: 400, message: "Market is not open for fills." },
  MARKET_ID_MISMATCH: { retryable: false, http_status: 400, message: "market_id must match the active round market." },
  INVALID_SIDE: { retryable: false, http_status: 400, message: "side must be buy_yes or sell_yes." },
  INVALID_PRICE_TENTHS: { retryable: false, http_status: 400, message: "price_tenths must be an integer between 1 and 999." },
  INVALID_SIZE_SHARES: { retryable: false, http_status: 400, message: "size_shares must be positive." },
  INVALID_EXPIRY_MS: { retryable: false, http_status: 400, message: "expiry_ms must be in the future." },
  INSUFFICIENT_COLLATERAL: { retryable: true, http_status: 400, message: "Insufficient collateral for order." },
  INVALID_MAKER_ADDRESS: { retryable: false, http_status: 400, message: "maker_address must be a valid EVM address for signed orders." },
  INVALID_SIGNATURE: { retryable: false, http_status: 400, message: "signature is not a valid maker order signature." },
  SIGNATURE_MISMATCH: { retryable: false, http_status: 400, message: "signature does not match maker_address." },
  NONCE_ALREADY_INVALIDATED: { retryable: false, http_status: 400, message: "nonce has already been invalidated for this maker." },
  ORDER_HASH_EXISTS: { retryable: false, http_status: 409, message: "order_hash already exists." },
  ORDER_HASH_REQUIRED: { retryable: false, http_status: 400, message: "order_hash is required." },
  ORDER_NOT_FOUND: { retryable: false, http_status: 404, message: "Order not found." },
  ORDER_NOT_OWNED_BY_AGENT: { retryable: false, http_status: 403, message: "Order does not belong to this agent." },
  ORDER_NOT_OPEN: { retryable: false, http_status: 400, message: "Order is not open." },
  ORDER_NOT_ACTIVE_MARKET: { retryable: false, http_status: 400, message: "Order is not fillable for the active market." },
  SELF_TRADE_BLOCKED: { retryable: false, http_status: 400, message: "Self-trading is not allowed." },
  ORDER_NO_REMAINING_SHARES: { retryable: false, http_status: 400, message: "Order has no remaining shares." },
  FILL_ONCHAIN_CONFIG_MISSING: { retryable: true, http_status: 503, message: "Onchain fill settlement is not configured." },
  FILL_REQUIRES_SIGNED_ORDER: { retryable: false, http_status: 400, message: "Onchain fills require a signed maker order." },
  FILL_ONCHAIN_FAILED: { retryable: true, http_status: 400, message: "Onchain fill failed." },
  FILL_ONCHAIN_REJECTED: { retryable: true, http_status: 400, message: "Onchain fill rejected." },
  CLAIM_MARKET_ID_REQUIRED: { retryable: false, http_status: 400, message: "market_id is required." },
  CLAIM_NO_POSITION: { retryable: false, http_status: 400, message: "No position for market." },
  CLAIM_MARKET_NOT_RESOLVED: { retryable: true, http_status: 400, message: "Market is not resolved." },
  CLAIM_NOTHING_CLAIMABLE: { retryable: false, http_status: 400, message: "No claimable balance remains." },
  CLAIM_CLAIMANT_ADDRESS_REQUIRED: { retryable: false, http_status: 400, message: "claimant_address is required for onchain claims." },
  CLAIM_ALREADY_ONCHAIN: { retryable: false, http_status: 409, message: "Claim already recorded onchain." },
  CLAIM_TX_FAILED: { retryable: true, http_status: 400, message: "Claim transaction failed." },
  CLAIM_ALREADY_SUBMITTED: { retryable: false, http_status: 409, message: "Claim already submitted." },
};

const agentError = (code, message = null, extra = {}) => {
  const definition = AGENT_ERROR_CODES[code] ?? null;
  return {
    ok: false,
    code,
    error: message ?? definition?.message ?? code,
    status: extra.status ?? definition?.http_status ?? 400,
    retryable: extra.retryable ?? definition?.retryable ?? false,
    ...extra,
  };
};

const buildAgentProtocolSchemas = () => ({
  protocol_version: AGENT_PROTOCOL_VERSION,
  enums: {
    scopes: [...DEFAULT_AGENT_SCOPES, "admin", "*"],
    notification_provider: ["discord", "telegram"],
    notification_event: AGENT_NOTIFICATION_EVENT_TYPES,
    round_status: ["waiting_for_open", "live", "resolved"],
    order_status: ["open", "partially_filled", "cancel_pending", "filled", "cancelled", "expired"],
    nonce_invalidation_status: [
      "pending_onchain",
      "submitted_local_stub",
      "confirmed_local_stub",
      "submitting_onchain",
      "submitted_onchain",
      "confirmed_onchain",
      "failed_onchain",
    ],
    order_side: ["buy_yes", "sell_yes"],
    stream_sync_mode: ["snapshot", "replay", "snapshot_after_gap"],
    recovery_reason: ["future_sequence", "gap_out_of_range"],
  },
  errors: AGENT_ERROR_CODES,
  idempotency: AGENT_IDEMPOTENCY,
  rate_limits: AGENT_RATE_LIMIT_POLICY,
  schemas: {
    "agent.bootstrap": {
      type: "object",
      required: ["server_now_ms", "agent_id", "market_id", "protocol_version", "auth", "streams", "capabilities"],
      properties: {
        server_now_ms: { type: "number" },
        agent_id: { type: ["string", "null"] },
        market_id: { type: ["string", "null"] },
        protocol_version: { type: "string" },
        current_round: { type: ["object", "null"] },
        book: { type: ["object", "null"] },
        positions: { type: "array" },
        feed_connected: { type: "boolean" },
        last_price: { type: ["number", "null"] },
        quote: { type: ["object", "null"] },
        auth: { type: "object" },
        streams: { type: "object" },
        capabilities: { type: "object" },
      },
    },
    "order.place": {
      type: "object",
      required: ["market_id", "side", "price_tenths", "size_shares"],
      properties: {
        market_id: { type: "string" },
        side: { enum: ["buy_yes", "sell_yes"] },
        price_tenths: { type: "integer", minimum: PRICE_TENTHS_MIN, maximum: PRICE_TENTHS_MAX },
        size_shares: { type: "number", exclusiveMinimum: 0 },
        expiry_ms: { type: "number" },
        client_order_id: { type: "string" },
        maker_address: { type: "string" },
        nonce: { type: "number" },
        signature: { type: "string" },
      },
    },
    "order.cancel": {
      type: "object",
      required: ["order_hash"],
      properties: {
        order_hash: { type: "string" },
      },
    },
    "order.take": {
      type: "object",
      required: ["order_hash", "size_shares"],
      properties: {
        order_hash: { type: "string" },
        size_shares: { type: "number", exclusiveMinimum: 0 },
        taker_address: { type: "string" },
      },
    },
    "claim.submit": {
      type: "object",
      required: ["market_id"],
      properties: {
        market_id: { type: "string" },
        claimant_address: { type: "string" },
      },
    },
    "stream.sync": {
      type: "object",
      required: ["stream", "mode", "server_now_ms"],
      properties: {
        stream: { type: "string" },
        mode: { enum: ["snapshot", "replay", "snapshot_after_gap"] },
        from_sequence: { type: ["number", "null"] },
        to_sequence: { type: ["number", "null"] },
        server_now_ms: { type: "number" },
      },
    },
    "stream.recovery_required": {
      type: "object",
      required: ["stream", "requested_sequence", "latest_sequence", "oldest_replayable_sequence", "reason", "server_now_ms"],
      properties: {
        stream: { type: "string" },
        requested_sequence: { type: "number" },
        latest_sequence: { type: "number" },
        oldest_replayable_sequence: { type: "number" },
        reason: { enum: ["future_sequence", "gap_out_of_range"] },
        server_now_ms: { type: "number" },
      },
    },
    "error": {
      type: "object",
      required: ["code", "error", "retryable"],
      properties: {
        code: { type: "string" },
        error: { type: "string" },
        retryable: { type: "boolean" },
      },
    },
    "notification.destination": {
      type: "object",
      required: ["provider"],
      properties: {
        provider: { enum: ["discord", "telegram"] },
        label: { type: "string" },
        webhook_url: { type: "string" },
        bot_token: { type: "string" },
        chat_id: { type: "string" },
        events: { type: "array", items: { enum: AGENT_NOTIFICATION_EVENT_TYPES } },
      },
    },
  },
});

const buildAgentProtocolDescriptor = () => ({
  protocol_version: AGENT_PROTOCOL_VERSION,
  generated_at: new Date().toISOString(),
  base_path: BASE_PATH,
  schema_path: AGENT_PROTOCOL_SCHEMAS_PATH,
  llms_path: `${BASE_PATH}/llms.txt`,
  endpoints: {
    me: `${BASE_PATH}/api/me`,
    account_summary: `${BASE_PATH}/api/account/summary`,
    markets: `${BASE_PATH}/api/markets`,
    markets_current: `${BASE_PATH}/api/markets/current`,
    stream_backfill: `${BASE_PATH}/api/streams/backfill`,
    order_by_hash: `${BASE_PATH}/api/orders/:order_hash`,
    order_place_rest: `${BASE_PATH}/api/orders/place`,
    order_cancel_rest: `${BASE_PATH}/api/orders/cancel`,
    order_take_rest: `${BASE_PATH}/api/orders/take`,
    notifications: `${BASE_PATH}/api/notifications`,
    notification_by_id: `${BASE_PATH}/api/notifications/:id`,
  },
  authentication: {
    preferred: "Authorization: Bearer YOUR_API_KEY",
    compatibility: ["x-pm-agent-key", "api_key query parameter"],
  },
  signing: {
    domain_name: ORDERBOOK_DOMAIN_NAME,
    domain_version: ORDERBOOK_DOMAIN_VERSION,
    chain_id: ORDER_SIGNING_CHAIN_ID,
    verifying_contract: ORDER_SIGNING_VERIFYING_CONTRACT,
    primary_type: ORDERBOOK_PRIMARY_TYPE,
  },
  idempotency: AGENT_IDEMPOTENCY,
  rate_limits: AGENT_RATE_LIMIT_POLICY,
  replay: {
    persistence_enabled: ledgerEnabled,
    retention_limit_per_stream: WS_REPLAY_HISTORY_LIMIT,
    durable_retention_limit_per_stream: STREAM_BACKFILL_RETENTION_LIMIT,
    storage: ledgerEnabled ? "postgres + in-memory cache" : "in-memory only",
  },
  notifications: {
    supported_providers: ["discord", "telegram"],
    supported_events: AGENT_NOTIFICATION_EVENT_TYPES,
  },
  schemas: buildAgentProtocolSchemas(),
});

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".jsx": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
};
const LLMS_TXT_PATH = join(PUBLIC_DIR, "llms.txt");

const resolvePublicFile = pathname => {
  let routedPath = pathname;
  if (routedPath === BASE_PATH) {
    routedPath = "/";
  } else if (routedPath.startsWith(`${BASE_PATH}/`)) {
    routedPath = routedPath.slice(BASE_PATH.length);
  }
  const cleanPath = routedPath === "/" ? "/index.html" : routedPath;
  const relative = normalize(cleanPath).replace(/^(\.\.[/\\])+/, "");
  const target = join(PUBLIC_DIR, relative);
  if (!target.startsWith(PUBLIC_DIR)) return null;
  return target;
};

const jsonResponse = (status, payload) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });

const buildLlmsTxt = () => {
  const signingContractLabel =
    ORDER_SIGNING_VERIFYING_CONTRACT === ORDERBOOK_DEFAULT_VERIFYING_CONTRACT
      ? `${ORDER_SIGNING_VERIFYING_CONTRACT} (default placeholder; onchain fill verification is not active in server execution)`
      : ORDER_SIGNING_VERIFYING_CONTRACT;
  const relayRpcLabel =
    NONCE_RELAY_MODE === "abstract_testnet" ? ABSTRACT_TESTNET_RPC_URL ?? "not configured" : "not required";
  return `Prediction Markets agent interface.

Generated at server startup: ${new Date().toISOString()}

Server configuration:
- Protocol version: ${AGENT_PROTOCOL_VERSION}
- Base path: ${BASE_PATH}
- Symbol: ${SYMBOL}
- Round duration: 5 minutes
- Public websocket: ${BASE_PATH}/ws
- Agent websocket: ${BASE_PATH}/ws/agent?api_key=YOUR_API_KEY
- Agent API authentication: prefer Authorization: Bearer YOUR_API_KEY
- Compatibility auth methods: x-pm-agent-key header or api_key query parameter
- REST status endpoint: ${BASE_PATH}/api/status
- Agent protocol JSON: ${AGENT_PROTOCOL_SCHEMA_PATH}
- Agent schemas JSON: ${AGENT_PROTOCOL_SCHEMAS_PATH}
- Agent identity endpoint: ${BASE_PATH}/api/me
- Account summary endpoint: ${BASE_PATH}/api/account/summary
- Notification endpoint: ${BASE_PATH}/api/notifications
- Stream backfill endpoint: ${BASE_PATH}/api/streams/backfill
- Nonce relay mode: ${NONCE_RELAY_MODE}
- Ledger enabled: ${ledgerEnabled}

Happy path:
1. Call GET ${BASE_PATH}/api/status and confirm status=ok
2. Call GET ${BASE_PATH}/api/markets/current and read current_round plus market_id
3. Connect to ${BASE_PATH}/ws/agent with an API key
4. Wait for agent.bootstrap
5. Send book.subscribe, positions.subscribe, and nonce_invalidations.subscribe
6. Read agent.bootstrap.payload.market_id, current_round.status, auth.scopes, and capabilities
7. While current_round.status is live, place orders with order.place or consume resting liquidity with order.take
8. After fills or cancellations, read positions.delta, fill.executed, order.cancelled, and nonce_invalidation.updated
9. After market.resolved, call GET ${BASE_PATH}/api/positions or submit claim.submit / POST ${BASE_PATH}/api/claims

Trading model:
- Market ids use the format ${SYMBOL}-5m-<round_start_ms>
- Only the active round can be traded
- Orders are accepted only while current_round.status is live
- Supported sides: buy_yes, sell_yes
- Price field: price_tenths integer from ${PRICE_TENTHS_MIN} to ${PRICE_TENTHS_MAX}
- Human price conversion: 543 means 54.3 cents
- size_shares must be positive
- Default expiry if omitted: round end + ${DEFAULT_ORDER_EXPIRY_BUFFER_MS} ms
- Self-trading is rejected on order.take
- Fills are currently offchain only

Order side rules:
- buy_yes means bidding for YES exposure
- sell_yes means offering YES exposure and is economically equivalent to buying NO exposure
- If the round resolves up, each net YES share settles to 1.0 USD and each net NO share settles to 0
- If the round resolves down, each net YES share settles to 0 and each net NO share settles to 1.0 USD
- A maker order keeps its original side when it is later filled by a taker
- A taker using order.take is consuming the maker's resting order, not specifying an opposite side field
- To take a buy_yes maker order, the taker is selling YES into that bid
- To take a sell_yes maker order, the taker is buying YES from that ask

Pricing and collateral rules:
- price_tenths is quoted on a 0 to 100.0 cent YES-price scale using tenths of a cent
- buy_yes collateral per share = price_tenths / 1000 USD
- sell_yes collateral per share = (1000 - price_tenths) / 1000 USD
- Examples:
  - buy_yes at 543 reserves 0.543 USD per share
  - sell_yes at 543 reserves 0.457 USD per share
- Reserved collateral is tracked from remaining open size, not original size
- Agent collateral checks are enforced only when agentApiKeys entries define collateralUsd
- Claims after resolution are based on net_yes_shares:
  - if round_result is up, positive net_yes_shares are claimable
  - if round_result is down, negative net_yes_shares become claimable NO exposure
  - flat currently yields no claimable payout

Time and unit conventions:
- All timestamps ending in _ms are Unix epoch milliseconds
- price_tenths is an integer in tenths of a cent on a 0 to 100.0 scale
- size_shares and fill_shares are decimal share quantities
- Signed orders convert size_shares to sizeMicroshares where 1 share = 1000000 microshares
- market_id embeds the round start timestamp in milliseconds

Status enums:
- Round status: waiting_for_open, live, resolved
- Order status: open, partially_filled, cancel_pending, filled, cancelled, expired
- Nonce invalidation status: pending_onchain, submitted_local_stub, confirmed_local_stub, submitting_onchain, submitted_onchain, confirmed_onchain, failed_onchain
- Fill role in GET ${BASE_PATH}/api/fills: maker or taker

Auth scopes:
- market:read
- orders:write
- orders:cancel
- fills:take
- claims:write
- nonce_invalidations:read
- admin or * bypasses per-scope checks and grants access to key-management endpoints

Signed maker order configuration:
- EIP-712 domain name: ${ORDERBOOK_DOMAIN_NAME}
- EIP-712 domain version: ${ORDERBOOK_DOMAIN_VERSION}
- Chain id for signed orders: ${ORDER_SIGNING_CHAIN_ID}
- Verifying contract for signed orders: ${signingContractLabel}
- Primary type: ${ORDERBOOK_PRIMARY_TYPE}
- Signed order fields:
  - marketId string
  - maker address
  - side string
  - priceTenths uint16
  - sizeMicroshares uint256
  - nonce uint64
  - expiryMs uint64
- size_shares is converted to sizeMicroshares using 1 share = 1000000 microshares
- For signed orders, maker_address must be a valid EVM address and the signature must recover to maker_address
- If fillManagerAddress is configured and orderSigningVerifyingContract is not set, the server uses fillManagerAddress as the EIP-712 verifying contract

Signing and settlement warning:
- The server currently verifies signed maker orders offchain using the configured EIP-712 domain
- Fill settlement mode: ${FILL_SETTLEMENT_MODE}
- Settlement / claim mode: ${SETTLEMENT_MODE}
- Fill manager address: ${FILL_MANAGER_ADDRESS ?? "not configured"}
- Settlement manager address: ${SETTLEMENT_MANAGER_ADDRESS ?? "not configured"}
- Agents should treat the signed order domain as the canonical hash/signature domain for maker orders

Nonce relay configuration:
- Relay mode: ${NONCE_RELAY_MODE}
- Abstract testnet RPC for relay: ${relayRpcLabel}
- Nonce manager address: ${NONCE_MANAGER_ADDRESS ?? "not configured"}
- Cancellation creates a nonce invalidation record immediately
- Relay status values may include:
  - pending_onchain
  - submitted_local_stub
  - confirmed_local_stub
  - submitting_onchain
  - submitted_onchain
  - confirmed_onchain
  - failed_onchain

Discovery flow for an agent:
1. Call GET ${BASE_PATH}/api/status
2. Call GET ${BASE_PATH}/api/markets/current
3. Connect to ${BASE_PATH}/ws/agent with an API key
4. Wait for agent.bootstrap
5. Subscribe to book, positions, and nonce invalidations
6. Place maker orders with order.place or execute resting liquidity with order.take

REST endpoints:
- GET ${BASE_PATH}/api/status
- GET ${BASE_PATH}/api/agent/protocol
- GET ${BASE_PATH}/api/agent/schemas
- GET ${BASE_PATH}/api/me
- GET ${BASE_PATH}/api/account/summary
- GET ${BASE_PATH}/api/streams/backfill?stream=<stream_key>&since_sequence=<n>&limit=<optional>
- GET ${BASE_PATH}/api/notifications
- POST ${BASE_PATH}/api/notifications
- PATCH ${BASE_PATH}/api/notifications/:id
- DELETE ${BASE_PATH}/api/notifications/:id
- GET ${BASE_PATH}/api/markets
- GET ${BASE_PATH}/api/markets/current
- GET ${BASE_PATH}/api/book?market_id=<optional>
- GET ${BASE_PATH}/api/resolutions?limit=<optional>&cursor=<optional>
- GET ${BASE_PATH}/api/admin/agent-keys
- POST ${BASE_PATH}/api/admin/agent-keys with JSON body { "agent_id", "label", "scopes", "collateral_usd"?, "rate_limit_per_minute"? }
- PATCH ${BASE_PATH}/api/admin/agent-keys/:key_id
- DELETE ${BASE_PATH}/api/admin/agent-keys/:key_id
- GET ${BASE_PATH}/api/orders
- GET ${BASE_PATH}/api/orders/:order_hash
- POST ${BASE_PATH}/api/orders/place
- POST ${BASE_PATH}/api/orders/cancel with JSON body { "order_hash": "ord_..." }
- POST ${BASE_PATH}/api/orders/take with JSON body { "order_hash": "ord_...", "size_shares": 1, "taker_address"?: "0x..." }
- GET ${BASE_PATH}/api/order-events
- GET ${BASE_PATH}/api/fills
- GET ${BASE_PATH}/api/claims
- POST ${BASE_PATH}/api/claims with JSON body { "market_id": "..." }
- GET ${BASE_PATH}/api/nonce-invalidations
- GET ${BASE_PATH}/api/positions

Agent websocket subscriptions:
- book.subscribe
- positions.subscribe
- nonce_invalidations.subscribe

Primary payload schemas:
- agent.bootstrap.payload:
  - server_now_ms number
  - agent_id string
  - market_id string | null
  - protocol_version string
  - current_round object | null
  - book object | null
  - positions array
  - feed_connected boolean
  - last_price number | null
  - quote object | null
  - auth object
  - streams object
  - capabilities object
- order object:
  - order_hash string
  - market_id string
  - maker_address string
  - agent_id string | null
  - side buy_yes | sell_yes
  - price_tenths integer
  - size_shares number
  - remaining_shares number
  - nonce number
  - expiry_ms number
  - client_order_id string | null
  - order_kind unsigned | signed
  - signature string | null
  - status open | partially_filled | filled | cancelled | expired
  - created_at_ms number
  - accepted_at_ms number
- fill.executed payload:
  - market_id string
  - order_hash string
  - maker_address string
  - maker_agent_id string | null
  - taker_address string
  - taker_agent_id string | null
  - side buy_yes | sell_yes
  - price_tenths integer
  - fill_shares number
  - remaining_shares number
  - status partially_filled | filled
  - server_now_ms number
- positions item:
  - market_id string
  - round_status string | null
  - round_result up | down | flat | null
  - open_buy_yes_shares number
  - open_sell_yes_shares number
  - reserved_collateral_usd number
  - open_order_count number
  - bought_yes_shares number
  - sold_yes_shares number
  - net_yes_shares number
  - fill_count number
  - claimed_usd number
  - gross_claimable_usd number
  - claimable_usd number
- book.delta payload:
  - market_id string
  - sequence number
  - reason string
  - side bids | asks
  - action upsert | remove
  - price_tenths integer
  - level object | null
  - updated_at_ms number
  - recovery object
- nonce_invalidation.updated payload:
  - id number
  - market_id string
  - maker_address string
  - nonce number
  - agent_id string | null
  - status string
  - relay_mode string | null
  - tx_hash string | null
  - error_message string | null
  - submitted_at_ms number | null
  - confirmed_at_ms number | null
  - created_at_ms number
- stream.sync payload:
  - stream string
  - mode snapshot | replay | snapshot_after_gap
  - from_sequence number | null
  - to_sequence number | null
  - server_now_ms number
- stream.recovery_required payload:
  - stream string
  - requested_sequence number
  - latest_sequence number
  - oldest_replayable_sequence number
  - reason future_sequence | gap_out_of_range
  - server_now_ms number

Agent websocket commands:

Subscribe to current book:
\`\`\`json
{ "type": "book.subscribe", "payload": { "since_sequence": 12 } }
\`\`\`

Subscribe to positions:
\`\`\`json
{ "type": "positions.subscribe", "payload": { "since_sequence": 4 } }
\`\`\`

Subscribe to nonce invalidations:
\`\`\`json
{ "type": "nonce_invalidations.subscribe", "payload": { "since_sequence": 2 } }
\`\`\`

Place an unsigned maker order:
\`\`\`json
{
  "type": "order.place",
  "payload": {
    "market_id": "${SYMBOL}-5m-<round_start_ms>",
    "side": "buy_yes",
    "price_tenths": 543,
    "size_shares": 10,
    "client_order_id": "agent-order-1"
  }
}
\`\`\`

Place a signed maker order:
\`\`\`json
{
  "type": "order.place",
  "payload": {
    "market_id": "${SYMBOL}-5m-<round_start_ms>",
    "maker_address": "0xYourMakerAddress",
    "side": "buy_yes",
    "price_tenths": 543,
    "size_shares": 10,
    "nonce": 12345,
    "expiry_ms": 1773377130000,
    "signature": "0x...",
    "client_order_id": "signed-order-1"
  }
}
\`\`\`

Cancel an order:
\`\`\`json
{
  "type": "order.cancel",
  "payload": {
    "order_hash": "ord_..."
  }
}
\`\`\`

Take a resting order:
\`\`\`json
{
  "type": "order.take",
  "payload": {
    "order_hash": "ord_...",
    "size_shares": 2,
    "taker_address": "0xYourTakerAddress"
  }
}
\`\`\`

Submit a claim after resolution:
\`\`\`json
{
  "type": "claim.submit",
  "payload": {
    "market_id": "${SYMBOL}-5m-<round_start_ms>"
  }
}
\`\`\`

Important websocket events:
- agent.bootstrap
- book.snapshot
- book.delta
- order.accepted
- order.ack
- order.reject
- order.cancelled
- fill.executed
- order.fill_ack
- nonce_invalidation.snapshot
- nonce_invalidation.updated
- positions.snapshot
- positions.delta
- market.state
- market.resolved
- trade.update
- quote.update
- feed.status
- stream.sync
- stream.recovery_required

Important response details:
- protocol_version is returned by GET ${BASE_PATH}/api/agent/protocol, GET ${BASE_PATH}/api/agent/schemas, and agent.bootstrap.payload.protocol_version
- agent.bootstrap.payload.capabilities reports order_place, order_cancel, order_take, fills_onchain, and nonce_relay_mode
- agent.bootstrap.payload.auth reports key metadata, scopes, and rate_limit_per_minute for the authenticated key
- agent.bootstrap.payload.streams reports current stream sequences and oldest replayable sequences for reconnect logic
- book.delta is a price-level delta, not a full snapshot
- replayable snapshot and delta payloads include payload.recovery metadata
- positions reports open orders, fills, net_yes_shares, reserved_collateral_usd, gross_claimable_usd, and claimable_usd
- order events are persisted and available via GET ${BASE_PATH}/api/order-events
- agent-owned Discord and Telegram destinations can receive order/fill/claim notifications

Common rejection conditions:
- order.place rejects when:
  - market is not open for trading
  - market_id does not match the active round
  - side is not buy_yes or sell_yes
  - price_tenths is not an integer between ${PRICE_TENTHS_MIN} and ${PRICE_TENTHS_MAX}
  - size_shares is not positive
  - expiry_ms is not in the future
  - collateral is insufficient for the configured agent
  - signed order maker_address is not a valid EVM address
  - signature is invalid or does not match maker_address
  - nonce has already been invalidated for this maker
  - order_hash already exists
- order.cancel rejects when:
  - order_hash is missing
  - order does not exist
  - order belongs to another agent
  - order status is no longer open or partially_filled
- order.take rejects when:
  - market is not open for fills
  - order_hash is missing
  - size_shares is not positive
  - order does not exist
  - order is not for the active market
  - order is no longer fillable
  - self-trading would occur
  - order has no remaining shares
- claim.submit rejects when:
  - market_id is missing
  - no position exists for the market
  - market is not resolved
  - no claimable balance remains
- any authenticated request may reject with 403 if the key is missing the required scope
- any authenticated request may reject with 429 if the key exceeds its configured rate_limit_per_minute

Deterministic error codes:
- Error payloads include code, error, and retryable
- The authoritative generated error-code list is exposed at GET ${BASE_PATH}/api/agent/schemas
- Important codes include:
  - AUTH_INVALID_API_KEY
  - AUTH_SCOPE_MISSING
  - RATE_LIMIT_EXCEEDED
  - MARKET_NOT_LIVE
  - INVALID_SIDE
  - INVALID_PRICE_TENTHS
  - INVALID_SIZE_SHARES
  - INVALID_EXPIRY_MS
  - INSUFFICIENT_COLLATERAL
  - INVALID_SIGNATURE
  - SIGNATURE_MISMATCH
  - NONCE_ALREADY_INVALIDATED
  - ORDER_HASH_EXISTS
  - ORDER_NOT_FOUND
  - ORDER_NOT_OWNED_BY_AGENT
  - SELF_TRADE_BLOCKED
  - CLAIM_MARKET_NOT_RESOLVED
  - CLAIM_NOTHING_CLAIMABLE

Idempotency rules:
- order.place supports client_order_id for client correlation only; it is not enforced as a uniqueness key
- Retrying an unsigned order.place with the same client_order_id creates another order
- Retrying a signed order.place with the same signed payload is effectively idempotent because duplicate order_hash values are rejected
- order.cancel is terminal rather than idempotent; retry only until you observe order.cancelled or a non-open order status
- claim.submit should be retried only after re-reading positions or claims

Rate-limit rules:
- Rate limiting is keyed by authenticated key_id using a fixed 60-second window
- The server responds with HTTP 429 / RATE_LIMIT_EXCEEDED when the configured per-key limit is exceeded
- Retry delay is included in the error message text

Local replay state guidance:
- Persist the last applied sequence number separately for each replayable stream key
- Recommended keys:
  - book:<market_id>
  - positions:<agent_id>:<market_id>
  - nonce_invalidations:<agent_id>:<market_id>
- On reconnect, resubscribe with since_sequence set to the last applied sequence
- After stream.recovery_required, discard incremental local state for that stream and rebuild from the replacement snapshot

Authority and restart guidance:
- The server is authoritative for the offchain book, fills, positions, claims, and nonce invalidation ledger state exposed by these endpoints
- Signed maker order hashes are authoritative under the configured EIP-712 domain
- Onchain settlement, fills, and claims depend on the configured settlement modes and reconciliation passes
- Agents should assume that replayable websocket state is sufficient only within the configured in-memory replay window; anything older must be rebuilt from snapshots and REST reads

Replay and recovery rules:
- Agent subscription streams are replayable with bounded in-memory history
- When the ledger is enabled, replay history is also persisted and rehydrated on server restart
- In-memory replay retention per stream: ${WS_REPLAY_HISTORY_LIMIT}
- Durable backfill retention per stream: ${STREAM_BACKFILL_RETENTION_LIMIT}
- Supported replayable streams:
  - book:<market_id>
  - positions:<agent_id>:<market_id>
  - nonce_invalidations:<agent_id>:<market_id>
- Subscription commands accept an optional since_sequence field
- If since_sequence is within the retained history window, the server replays missed events and then emits stream.sync with mode replay
- If since_sequence is missing, the server emits a fresh snapshot and then stream.sync with mode snapshot
- If since_sequence is too old or ahead of the current stream, the server emits stream.recovery_required, then a fresh snapshot, then stream.sync with mode snapshot_after_gap
- Clients should treat stream.recovery_required as a signal that local incremental state is no longer trustworthy without the replacement snapshot
- For long gaps outside the retained replay window, agents can query GET ${BASE_PATH}/api/streams/backfill using the stream key plus since_sequence

Connection notes:
- Public market data websocket does not require auth: ${BASE_PATH}/ws
- Agent trading websocket requires an API key supplied out of band; this document does not expose secrets
- Query-string api_key support exists for compatibility and local testing, but Authorization: Bearer is preferred
- If you need the current active market_id, read agent.bootstrap.payload.market_id or GET ${BASE_PATH}/api/markets/current
- Agents may use REST mutations for place/cancel/take or the agent websocket; websocket remains the source for live event flow

Agent notifications:
- Supported providers: discord, telegram
- Supported event filters:
  - order.accepted
  - order.cancelled
  - fill.executed
  - claim.accepted
- Discord destinations use a webhook_url
- Telegram destinations use bot_token plus chat_id
- Notification deliveries are best-effort and asynchronous
`;
};

const writeGeneratedLlmsTxt = () => {
  const content = buildLlmsTxt();
  writeFileSync(LLMS_TXT_PATH, content, "utf8");
  return content;
};

const publicClients = new Set();
const agentClients = new Set();
const ledgerPool = LEDGER_DATABASE_URL ? new Pool({ connectionString: LEDGER_DATABASE_URL }) : null;
const ledgerEnabled = Boolean(ledgerPool);
writeGeneratedLlmsTxt();
const hashApiKey = value => createHash("sha256").update(String(value)).digest("hex");
const buildAgentScopeSet = scopes => {
  const items = Array.isArray(scopes) ? scopes.filter(Boolean).map(String) : [];
  return new Set(items.length ? items : DEFAULT_AGENT_SCOPES);
};
const scopeSetToArray = scopeSet => [...scopeSet].sort();
const parseOptionalFiniteNumber = value => {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};
const normalizeAgentAuthRecord = ({
  id,
  keyId = null,
  keyPrefix = null,
  agentId,
  label,
  collateralUsd,
  claimantAddress = null,
  scopes = DEFAULT_AGENT_SCOPES,
  rateLimitPerMinute = null,
  source = "config",
}) => ({
  id: id ?? `agent:${agentId}`,
  keyId: keyId ?? id ?? `agent:${agentId}`,
  keyPrefix: keyPrefix ?? null,
  agentId,
  label: label ?? agentId ?? "agent",
  collateralUsd: parseOptionalFiniteNumber(collateralUsd),
  claimantAddress: claimantAddress && isAddress(claimantAddress) ? getAddress(claimantAddress) : null,
  scopes: buildAgentScopeSet(scopes),
  rateLimitPerMinute:
    parseOptionalFiniteNumber(rateLimitPerMinute) === null
      ? null
      : Math.max(1, Math.floor(parseOptionalFiniteNumber(rateLimitPerMinute))),
  source,
});
const parseConfiguredAgentApiKey = entry => {
  if (typeof entry === "string" && entry) {
    return {
      rawKey: entry,
      record: normalizeAgentAuthRecord({
        agentId: entry,
        label: entry,
      }),
    };
  }
  const rawKey = entry?.key ? String(entry.key) : null;
  if (!rawKey) return null;
  return {
    rawKey,
    record: normalizeAgentAuthRecord({
      keyPrefix: rawKey.slice(0, 8),
      agentId: entry?.agentId ?? entry?.label ?? rawKey,
      label: entry?.label ?? entry?.agentId ?? "agent",
      collateralUsd: entry?.collateralUsd,
      claimantAddress: entry?.claimantAddress ?? entry?.walletAddress ?? null,
      scopes: entry?.scopes,
      rateLimitPerMinute: entry?.rateLimitPerMinute,
      source: "config",
    }),
  };
};
const CONFIGURED_AGENT_KEY_ENTRIES = (Array.isArray(APP_CONFIG.agentApiKeys) ? APP_CONFIG.agentApiKeys : [])
  .map(parseConfiguredAgentApiKey)
  .filter(Boolean);
const DEV_AGENT_KEYS = new Map(CONFIGURED_AGENT_KEY_ENTRIES.map(entry => [entry.rawKey, entry.record]));
const authCache = new Map();
const authRateWindows = new Map();
const authLastUsedWriteAt = new Map();
let upstreamSocket = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let secondTimer = null;
let nonceRelayTimer = null;
let reconciliationTimer = null;
let reconciliationRunning = false;
let monitoringTimer = null;
let ledgerWriteChain = Promise.resolve();
let nonceRelayClients = null;
let abstractRelayClients = null;
const blockTimestampCache = new Map();

const state = {
  feedConnected: false,
  lastTrade: APP_CONFIG.initialTrade ?? null,
  lastPrice: Number.isFinite(Number(APP_CONFIG.initialLastPrice)) ? Number(APP_CONFIG.initialLastPrice) : null,
  quote: APP_CONFIG.initialQuote ?? null,
  chartSeries: [],
  currentRound: APP_CONFIG.initialRound ?? null,
  previousRound: null,
  resolvedRounds: [],
  startedAt: Date.now(),
  monitoring: {
    lastFeedEventAtMs:
      APP_CONFIG.initialTrade?.timeMs ?? APP_CONFIG.initialQuote?.timeMs ?? null,
    lastReconciliationStartedAtMs: null,
    lastReconciliationSuccessAtMs: null,
    lastReconciliationFailureAtMs: null,
    lastReconciliationError: null,
    relayWalletBalanceEth: null,
    recentOnchainFailures: [],
    recentWebsocketDisconnects: [],
    activeAlertCodes: [],
    lastAlertEvaluationAtMs: null,
  },
};
const books = new Map();
const agentStreamState = new Map();

const pruneRecentOnchainFailures = nowMs => {
  const lookbackMs = Number(MONITORING_THRESHOLDS?.onchainFailureLookbackMs) || 300_000;
  state.monitoring.recentOnchainFailures = state.monitoring.recentOnchainFailures.filter(
    entry => nowMs - entry.atMs <= lookbackMs,
  );
};

const pruneRecentWebsocketDisconnects = nowMs => {
  const lookbackMs = Number(MONITORING_THRESHOLDS?.websocketDisconnectLookbackMs) || 60_000;
  state.monitoring.recentWebsocketDisconnects = state.monitoring.recentWebsocketDisconnects.filter(
    entry => nowMs - entry.atMs <= lookbackMs,
  );
};

const recordWebsocketDisconnect = kind => {
  const atMs = Date.now();
  state.monitoring.recentWebsocketDisconnects.push({ kind, atMs });
  pruneRecentWebsocketDisconnects(atMs);
};

const recordOnchainFailure = ({ kind, error }) => {
  const atMs = Date.now();
  const message = error instanceof Error ? error.message : String(error ?? "unknown error");
  state.monitoring.recentOnchainFailures.push({ kind, message, atMs });
  pruneRecentOnchainFailures(atMs);
};

const fetchRelayWalletBalanceEth = async () => {
  if (!ABSTRACT_TESTNET_RPC_URL || !NONCE_RELAY_PRIVATE_KEY) return null;
  try {
    const { publicClient, account } = createAbstractRelayClients();
    const balanceWei = await publicClient.getBalance({ address: account.address });
    return Number(balanceWei) / 1e18;
  } catch {
    return null;
  }
};

const getMonitoringLedgerMetrics = async () => {
  if (!ledgerPool) {
    return {
      pendingNonceInvalidations: 0,
      oldestPendingNonceCreatedAtMs: null,
      failedNonceInvalidations: 0,
    };
  }
  const pendingStatuses = ["pending_onchain", "submitting_onchain", "submitted_onchain"];
  const [pendingResult, failedResult] = await Promise.all([
    ledgerPool.query(
      `
        select count(*)::int as count, min(extract(epoch from created_at) * 1000)::bigint as oldest_created_at_ms
        from prediction_market_nonce_invalidations
        where status = any($1::text[])
      `,
      [pendingStatuses],
    ),
    ledgerPool.query(
      `
        select count(*)::int as count
        from prediction_market_nonce_invalidations
        where status = 'failed_onchain'
      `,
    ),
  ]);
  return {
    pendingNonceInvalidations: Number(pendingResult.rows[0]?.count ?? 0),
    oldestPendingNonceCreatedAtMs: parseNullableNumber(pendingResult.rows[0]?.oldest_created_at_ms),
    failedNonceInvalidations: Number(failedResult.rows[0]?.count ?? 0),
  };
};

const buildRuntimeMonitoringSnapshot = async () => {
  const nowMs = Date.now();
  pruneRecentOnchainFailures(nowMs);
  pruneRecentWebsocketDisconnects(nowMs);
  const ledgerMetrics = await getMonitoringLedgerMetrics();
  const relayWalletBalanceEth = await fetchRelayWalletBalanceEth();
  state.monitoring.relayWalletBalanceEth = relayWalletBalanceEth;
  const snapshot = buildMonitoringSnapshot({
    nowMs,
    feedConnected: state.feedConnected,
    lastFeedEventAtMs: state.monitoring.lastFeedEventAtMs,
    reconciliationEnabled: shouldRunChainReconciliation(),
    lastReconciliationSuccessAtMs: state.monitoring.lastReconciliationSuccessAtMs,
    lastReconciliationFailureAtMs: state.monitoring.lastReconciliationFailureAtMs,
    lastReconciliationError: state.monitoring.lastReconciliationError,
    pendingNonceInvalidations: ledgerMetrics.pendingNonceInvalidations,
    oldestPendingNonceCreatedAtMs: ledgerMetrics.oldestPendingNonceCreatedAtMs,
    failedNonceInvalidations: ledgerMetrics.failedNonceInvalidations,
    relayWalletBalanceEth,
    recentOnchainFailureCount: state.monitoring.recentOnchainFailures.length,
    recentWebsocketDisconnectCount: state.monitoring.recentWebsocketDisconnects.length,
    websocketPublicClientCount: publicClients.size,
    websocketAgentClientCount: agentClients.size,
    thresholds: MONITORING_THRESHOLDS,
  });
  return {
    ...snapshot,
    recentOnchainFailures: state.monitoring.recentOnchainFailures.slice(-10),
    recentWebsocketDisconnects: state.monitoring.recentWebsocketDisconnects.slice(-25),
  };
};

const sendMonitoringWebhook = async alerts => {
  if (!MONITORING_WEBHOOK_URL || !Array.isArray(alerts) || alerts.length === 0) return;
  try {
    await fetch(MONITORING_WEBHOOK_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        app: "prediction-markets",
        server_now_ms: Date.now(),
        alerts,
      }),
    });
  } catch (error) {
    console.error("[prediction-markets] monitoring webhook failed", error);
  }
};

const processMonitoringTick = async () => {
  const snapshot = await buildRuntimeMonitoringSnapshot();
  const alerts = evaluateMonitoringAlerts(snapshot);
  state.monitoring.lastAlertEvaluationAtMs = snapshot.nowMs;
  const nextCodes = alerts.map(alert => alert.code).sort();
  const prevCodes = [...state.monitoring.activeAlertCodes].sort();
  if (JSON.stringify(nextCodes) !== JSON.stringify(prevCodes)) {
    state.monitoring.activeAlertCodes = nextCodes;
    if (alerts.length > 0) {
      await sendMonitoringWebhook(alerts);
    }
  }
};

const buildStreamKey = ({ kind, marketId = null, agentId = null }) => [kind, agentId ?? "*", marketId ?? "*"].join(":");
const parseStreamKey = streamKey => {
  if (!streamKey || typeof streamKey !== "string") return null;
  const [kind, agentId = null, marketId = null] = streamKey.split(":");
  if (!kind) return null;
  return {
    kind,
    agentId: agentId === "*" ? null : agentId ?? null,
    marketId: marketId === "*" ? null : marketId ?? null,
  };
};
const ensureAgentStream = streamKey => {
  let stream = agentStreamState.get(streamKey);
  if (!stream) {
    stream = {
      sequence: 0,
      history: [],
    };
    agentStreamState.set(streamKey, stream);
  }
  return stream;
};
const streamOldestReplayableSequence = streamKey => {
  const stream = agentStreamState.get(streamKey);
  if (!stream?.history?.length) return stream?.sequence ?? 0;
  return stream.history[0].sequence;
};
const appendAgentStreamEvent = ({ streamKey, type, payload, sequence = null, persist = true }) => {
  const stream = ensureAgentStream(streamKey);
  if (Number.isInteger(sequence)) {
    stream.sequence = Math.max(stream.sequence, sequence);
  } else {
    stream.sequence += 1;
  }
  const entry = {
    sequence: stream.sequence,
    type,
    payload,
  };
  stream.history.push(entry);
  if (stream.history.length > WS_REPLAY_HISTORY_LIMIT) {
    stream.history.splice(0, stream.history.length - WS_REPLAY_HISTORY_LIMIT);
  }
  if (persist && ledgerPool) {
    void enqueueLedgerWrite(async () => {
      await ledgerPool.query(
        `
          insert into prediction_market_stream_events (
            stream_key,
            sequence,
            event_type,
            payload,
            created_at
          )
          values ($1,$2,$3,$4, now())
          on conflict (stream_key, sequence) do update set
            event_type = excluded.event_type,
            payload = excluded.payload,
            created_at = now()
        `,
        [streamKey, entry.sequence, type, JSON.stringify(payload ?? {})],
      );
      await ledgerPool.query(
        `
          delete from prediction_market_stream_events
          where stream_key = $1
            and sequence < (
              select coalesce(max(sequence), 0) - $2
              from prediction_market_stream_events
              where stream_key = $1
            )
        `,
        [streamKey, STREAM_BACKFILL_RETENTION_LIMIT],
      );
    });
  }
  return entry;
};
const replayAgentStreamEvents = ({ streamKey, sinceSequence }) => {
  const stream = ensureAgentStream(streamKey);
  return stream.history.filter(entry => entry.sequence > sinceSequence);
};

const listPersistedStreamEvents = async ({
  streamKey,
  sinceSequence = null,
  limit = Math.min(1000, STREAM_BACKFILL_RETENTION_LIMIT),
}) => {
  if (!ledgerPool || !streamKey) return [];
  const safeLimit = Math.max(1, Math.min(1000, Math.floor(Number(limit) || Math.min(1000, STREAM_BACKFILL_RETENTION_LIMIT))));
  const normalizedSince = parseIntegerOrNull(sinceSequence) ?? 0;
  const { rows } = await ledgerPool.query(
    `
      select stream_key, sequence, event_type, payload, extract(epoch from created_at) * 1000 as created_at_ms
      from prediction_market_stream_events
      where stream_key = $1
        and sequence > $2
      order by sequence asc
      limit $3
    `,
    [streamKey, normalizedSince, safeLimit],
  );
  return rows.map(row => ({
    stream: row.stream_key,
    sequence: Number(row.sequence),
    type: row.event_type,
    payload: row.payload ?? {},
    created_at_ms: Number(row.created_at_ms),
  }));
};

const getCurrentMarketId = round => (round?.startMs ? `${SYMBOL}-5m-${round.startMs}` : null);
const normalizeUsd = value => Number(Number(value).toFixed(6));
const microusdToUsd = value => normalizeUsd(Number(value ?? 0n) / 1_000_000);
const computeOrderCollateralUsd = ({ side, priceTenths, remainingShares }) => {
  const shares = Number(remainingShares);
  const price = Number(priceTenths);
  if (!Number.isFinite(shares) || shares <= 0) return 0;
  if (!Number.isFinite(price)) return 0;
  const unitCost = side === "sell_yes" ? (1000 - price) / 1000 : price / 1000;
  return normalizeUsd(Math.max(0, shares * unitCost));
};

const buildEmptyBook = marketId => ({
  marketId,
  bids: new Map(),
  asks: new Map(),
  orders: new Map(),
  sequence: 0,
  updatedAtMs: Date.now(),
});

const ensureBook = marketId => {
  let book = books.get(marketId);
  if (!book) {
    book = buildEmptyBook(marketId);
    books.set(marketId, book);
  }
  return book;
};

const mapSideToBookKey = side => (side === "sell_yes" ? "asks" : "bids");

const sortPriceLevels = levels => [...levels.keys()].sort((a, b) => a - b);
const parseIntegerOrNull = value => {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isInteger(numeric) ? numeric : null;
};

const serializeBook = marketId => {
  const streamKey = buildStreamKey({ kind: "book", marketId });
  const stream = ensureAgentStream(streamKey);
  const book = books.get(marketId) ?? buildEmptyBook(marketId);
  const effectiveSequence = Math.max(book.sequence ?? 0, stream.sequence ?? 0);
  const serializeLevels = levels =>
    sortPriceLevels(levels).map(priceTenths => {
      const orders = levels.get(priceTenths) ?? [];
      const sizeShares = orders.reduce((sum, order) => sum + Number(order.remaining_shares ?? order.size_shares ?? 0), 0);
      return {
        price_tenths: priceTenths,
        order_count: orders.length,
        size_shares: Number(sizeShares.toFixed(8)),
      };
    });
  return {
    market_id: marketId,
    sequence: effectiveSequence,
    bids: serializeLevels(book.bids).sort((a, b) => b.price_tenths - a.price_tenths),
    asks: serializeLevels(book.asks).sort((a, b) => a.price_tenths - b.price_tenths),
    updated_at_ms: book.updatedAtMs,
    recovery: {
      stream: streamKey,
      sequence: effectiveSequence,
      snapshot: true,
      oldest_replayable_sequence: streamOldestReplayableSequence(streamKey),
    },
  };
};

const serializeBookLevel = (marketId, sideKey, priceTenths) => {
  const book = books.get(marketId) ?? buildEmptyBook(marketId);
  const levels = book[sideKey];
  const orders = levels.get(priceTenths) ?? [];
  if (!orders.length) return null;
  const sizeShares = orders.reduce((sum, order) => sum + Number(order.remaining_shares ?? order.size_shares ?? 0), 0);
  return {
    price_tenths: Number(priceTenths),
    order_count: orders.length,
    size_shares: Number(sizeShares.toFixed(8)),
  };
};

const upsertBookOrder = order => {
  if (!order?.market_id || !DEFAULT_OPEN_ORDER_STATUSES.has(order.status)) return;
  const book = ensureBook(order.market_id);
  const sideKey = mapSideToBookKey(order.side);
  const levels = book[sideKey];
  const priceTenths = Number(order.price_tenths);
  const existing = book.orders.get(order.order_hash);
  if (existing) {
    const prevLevels = book[mapSideToBookKey(existing.side)];
    const prevPrice = Number(existing.price_tenths);
    const prevOrders = prevLevels.get(prevPrice) ?? [];
    prevLevels.set(prevPrice, prevOrders.filter(entry => entry.order_hash !== existing.order_hash));
    if ((prevLevels.get(prevPrice) ?? []).length === 0) {
      prevLevels.delete(prevPrice);
    }
  }
  const priceLevelOrders = levels.get(priceTenths) ?? [];
  priceLevelOrders.push(order);
  priceLevelOrders.sort((a, b) => Number(a.accepted_at_ms ?? a.created_at_ms ?? 0) - Number(b.accepted_at_ms ?? b.created_at_ms ?? 0));
  levels.set(priceTenths, priceLevelOrders);
  book.orders.set(order.order_hash, order);
  book.sequence += 1;
  book.updatedAtMs = Date.now();
};

const removeBookOrder = ({ market_id, order_hash }) => {
  const book = books.get(market_id);
  if (!book) return;
  const existing = book.orders.get(order_hash);
  if (!existing) return;
  const sideKey = mapSideToBookKey(existing.side);
  const levels = book[sideKey];
  const priceTenths = Number(existing.price_tenths);
  const nextOrders = (levels.get(priceTenths) ?? []).filter(entry => entry.order_hash !== order_hash);
  if (nextOrders.length) {
    levels.set(priceTenths, nextOrders);
  } else {
    levels.delete(priceTenths);
  }
  book.orders.delete(order_hash);
  book.sequence += 1;
  book.updatedAtMs = Date.now();
};

const clearBook = marketId => {
  const nextBook = buildEmptyBook(marketId);
  const existing = books.get(marketId);
  nextBook.sequence = (existing?.sequence ?? 0) + 1;
  nextBook.updatedAtMs = Date.now();
  books.set(marketId, nextBook);
};

const broadcastPublic = payload => {
  const message = JSON.stringify(payload);
  for (const client of publicClients) {
    try {
      client.send(message);
    } catch {
      publicClients.delete(client);
      try {
        client.close();
      } catch {}
    }
  }
};

const broadcastAgent = (payload, filter = null) => {
  const message = JSON.stringify(payload);
  for (const client of agentClients) {
    try {
      if (filter && !filter(client)) continue;
      client.send(message);
    } catch {
      agentClients.delete(client);
      try {
        client.close();
      } catch {}
    }
  }
};

const broadcastNonceInvalidationUpdate = payload => {
  const streamKey = buildStreamKey({
    kind: "nonce_invalidations",
    marketId: payload.market_id,
    agentId: payload.agent_id,
  });
  const entry = appendAgentStreamEvent({
    streamKey,
    type: "nonce_invalidation.updated",
    payload,
  });
  broadcastAgent(
    {
      type: entry.type,
      payload: {
        ...entry.payload,
        recovery: {
          stream: streamKey,
          sequence: entry.sequence,
          previous_sequence: entry.sequence - 1,
          replayed: false,
        },
      },
    },
    client => client.data?.agent?.agentId === payload.agent_id,
  );
};

const broadcastBook = marketId => {
  const snapshot = serializeBook(marketId);
  broadcastAgent(
    {
      type: "book.snapshot",
      payload: snapshot,
    },
    client => client.data?.subscriptions?.books?.has(marketId),
  );
};

const broadcastBookDelta = ({ marketId, reason, order = null, side = null, priceTenths = null }) => {
  const snapshot = serializeBook(marketId);
  const streamKey = buildStreamKey({ kind: "book", marketId });
  const normalizedSide = side ?? (order?.side === "sell_yes" ? "asks" : order?.side === "buy_yes" ? "bids" : null);
  const normalizedPriceTenths = parseIntegerOrNull(priceTenths) ?? parseIntegerOrNull(order?.price_tenths);
  const level =
    normalizedSide && Number.isInteger(normalizedPriceTenths)
      ? serializeBookLevel(marketId, normalizedSide, normalizedPriceTenths)
      : null;
  appendAgentStreamEvent({
    streamKey,
    type: "book.delta",
    sequence: snapshot.sequence,
    payload: {
      market_id: marketId,
      sequence: snapshot.sequence,
      reason,
      order,
      side: normalizedSide,
      action: level ? "upsert" : "remove",
      level,
      price_tenths: normalizedPriceTenths,
      updated_at_ms: snapshot.updated_at_ms,
    },
  });
  broadcastAgent(
    {
      type: "book.delta",
      payload: {
        market_id: marketId,
        sequence: snapshot.sequence,
        reason,
        order,
        side: normalizedSide,
        action: level ? "upsert" : "remove",
        level,
        price_tenths: normalizedPriceTenths,
        updated_at_ms: snapshot.updated_at_ms,
        recovery: {
          stream: streamKey,
          sequence: snapshot.sequence,
          previous_sequence: Math.max(0, snapshot.sequence - 1),
          replayed: false,
          oldest_replayable_sequence: streamOldestReplayableSequence(streamKey),
        },
      },
    },
    client => client.data?.subscriptions?.books?.has(marketId),
  );
};

const normalizeOpenOrder = row => ({
  order_hash: row.order_hash,
  market_id: row.market_id,
  maker_address: row.maker_address,
  agent_id: row.agent_id,
  side: row.side,
  price_tenths: Number(row.price_tenths),
  size_shares: Number(row.size_shares),
  remaining_shares: Number(row.remaining_shares),
  nonce: Number(row.nonce),
  expiry_ms: Number(row.expiry_ms),
  status: row.status,
  client_order_id: row.client_order_id,
  order_kind: row.order_kind ?? "unsigned",
  signature: row.signature ?? null,
  created_at_ms: Number(row.created_at_ms),
  accepted_at_ms: Number(row.accepted_at_ms),
});

const createOrderHash = () => `ord_${Date.now().toString(36)}_${crypto.randomUUID().replace(/-/g, "").slice(0, 16)}`;

const enqueueLedgerWrite = task => {
  if (!ledgerEnabled) return Promise.resolve();
  ledgerWriteChain = ledgerWriteChain
    .then(task)
    .catch(error => {
      console.error("[prediction-markets] ledger write failed", error);
    });
  return ledgerWriteChain;
};

const ensureLedgerSchema = async () => {
  if (!ledgerPool) return;
  await ledgerPool.query(`
    create table if not exists prediction_market_rounds (
      venue text not null,
      symbol text not null,
      start_ms bigint not null,
      end_ms bigint not null,
      target_price double precision null,
      current_price double precision null,
      close_price double precision null,
      status text not null,
      result text null,
      first_trade_time_ms bigint null,
      resolved_at_ms bigint null,
      resolution_tx_hash text null,
      resolution_mode text null,
      updated_at timestamptz not null default now(),
      created_at timestamptz not null default now(),
      primary key (venue, symbol, start_ms)
    )
  `);
  await ledgerPool.query(`
    alter table prediction_market_rounds
    add column if not exists resolution_tx_hash text null
  `);
  await ledgerPool.query(`
    alter table prediction_market_rounds
    add column if not exists resolution_mode text null
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_orders (
      order_hash text primary key,
      market_id text not null,
      maker_address text not null,
      agent_id text null,
      side text not null,
      price_tenths integer not null,
      size_shares numeric not null,
      remaining_shares numeric not null,
      nonce bigint not null,
      expiry_ms bigint not null,
      client_order_id text null,
      order_kind text not null default 'unsigned',
      signature text null,
      status text not null,
      created_at_ms bigint not null,
      accepted_at_ms bigint not null,
      updated_at timestamptz not null default now(),
      created_at timestamptz not null default now()
    )
  `);
  await ledgerPool.query(`
    alter table prediction_market_orders
    add column if not exists order_kind text not null default 'unsigned'
  `);
  await ledgerPool.query(`
    alter table prediction_market_orders
    add column if not exists signature text null
  `);
  await ledgerPool.query(`
    create index if not exists prediction_market_orders_market_status_idx
    on prediction_market_orders (market_id, status, price_tenths, accepted_at_ms)
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_order_events (
      id bigserial primary key,
      order_hash text not null,
      market_id text not null,
      event_type text not null,
      payload jsonb not null,
      created_at timestamptz not null default now()
    )
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_nonce_invalidations (
      id bigserial primary key,
      market_id text not null,
      maker_address text not null,
      nonce bigint not null,
      agent_id text null,
      status text not null,
      relay_mode text null,
      tx_hash text null,
      error_message text null,
      submitted_at_ms bigint null,
      confirmed_at_ms bigint null,
      created_at timestamptz not null default now()
    )
  `);
  await ledgerPool.query(`
    alter table prediction_market_nonce_invalidations
    add column if not exists relay_mode text null
  `);
  await ledgerPool.query(`
    alter table prediction_market_nonce_invalidations
    add column if not exists tx_hash text null
  `);
  await ledgerPool.query(`
    alter table prediction_market_nonce_invalidations
    add column if not exists error_message text null
  `);
  await ledgerPool.query(`
    alter table prediction_market_nonce_invalidations
    add column if not exists submitted_at_ms bigint null
  `);
  await ledgerPool.query(`
    alter table prediction_market_nonce_invalidations
    add column if not exists confirmed_at_ms bigint null
  `);
  await ledgerPool.query(`
    create index if not exists prediction_market_nonce_invalidations_maker_nonce_idx
    on prediction_market_nonce_invalidations (maker_address, nonce)
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_fills (
      id bigserial primary key,
      market_id text not null,
      order_hash text not null,
      maker_address text not null,
      maker_agent_id text null,
      taker_address text not null,
      taker_agent_id text null,
      side text not null,
      price_tenths integer not null,
      fill_shares numeric not null,
      tx_hash text null,
      created_at timestamptz not null default now()
    )
  `);
  await ledgerPool.query(`
    alter table prediction_market_fills
    add column if not exists maker_agent_id text null
  `);
  await ledgerPool.query(`
    alter table prediction_market_fills
    add column if not exists taker_agent_id text null
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_claims (
      id bigserial primary key,
      market_id text not null,
      agent_id text not null,
      claimant_address text null,
      amount_usd numeric not null,
      round_result text not null,
      tx_hash text null,
      claim_mode text null,
      created_at_ms bigint not null,
      created_at timestamptz not null default now(),
      unique (market_id, agent_id)
    )
  `);
  await ledgerPool.query(`
    alter table prediction_market_claims
    add column if not exists claimant_address text null
  `);
  await ledgerPool.query(`
    alter table prediction_market_claims
    add column if not exists tx_hash text null
  `);
  await ledgerPool.query(`
    alter table prediction_market_claims
    add column if not exists claim_mode text null
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_api_keys (
      id bigserial primary key,
      agent_id text not null,
      label text not null,
      key_prefix text not null,
      key_hash text not null unique,
      scopes jsonb not null default '[]'::jsonb,
      collateral_usd numeric null,
      rate_limit_per_minute integer null,
      status text not null default 'active',
      last_used_at timestamptz null,
      revoked_at timestamptz null,
      source text not null default 'managed',
      updated_at timestamptz not null default now(),
      created_at timestamptz not null default now()
    )
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists scopes jsonb not null default '[]'::jsonb
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists collateral_usd numeric null
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists rate_limit_per_minute integer null
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists status text not null default 'active'
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists last_used_at timestamptz null
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists revoked_at timestamptz null
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists source text not null default 'managed'
  `);
  await ledgerPool.query(`
    alter table prediction_market_api_keys
    add column if not exists claimant_address text null
  `);
  await ledgerPool.query(`
    create index if not exists prediction_market_api_keys_agent_status_idx
    on prediction_market_api_keys (agent_id, status)
  `);
  await ledgerPool.query(`
    create index if not exists prediction_market_api_keys_prefix_idx
    on prediction_market_api_keys (key_prefix)
  `);
  await ledgerPool.query(`
    create index if not exists prediction_market_api_keys_claimant_address_idx
    on prediction_market_api_keys (lower(claimant_address))
  `);
  await ledgerPool.query(`
    create unique index if not exists prediction_market_fills_tx_hash_uidx
    on prediction_market_fills (tx_hash)
    where tx_hash is not null
  `);
  await ledgerPool.query(`
    create unique index if not exists prediction_market_claims_tx_hash_uidx
    on prediction_market_claims (tx_hash)
    where tx_hash is not null
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_chain_cursors (
      stream text primary key,
      last_block_number bigint not null default 0,
      updated_at timestamptz not null default now()
    )
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_stream_events (
      stream_key text not null,
      sequence bigint not null,
      event_type text not null,
      payload jsonb not null default '{}'::jsonb,
      created_at timestamptz not null default now(),
      primary key (stream_key, sequence)
    )
  `);
  await ledgerPool.query(`
    create index if not exists prediction_market_stream_events_stream_created_idx
    on prediction_market_stream_events (stream_key, created_at desc)
  `);
  await ledgerPool.query(`
    create table if not exists prediction_market_agent_notifications (
      id bigserial primary key,
      agent_id text not null,
      provider text not null,
      label text null,
      status text not null default 'active',
      webhook_url text null,
      telegram_bot_token text null,
      telegram_chat_id text null,
      events jsonb not null default '[]'::jsonb,
      last_delivery_at timestamptz null,
      last_error text null,
      updated_at timestamptz not null default now(),
      created_at timestamptz not null default now()
    )
  `);
  await ledgerPool.query(`
    create index if not exists prediction_market_agent_notifications_agent_status_idx
    on prediction_market_agent_notifications (agent_id, status)
  `);
};

const parseNullableNumber = value => {
  if (value === null || value === undefined) return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const normalizeScopeList = value => (Array.isArray(value) ? value.filter(Boolean).map(String) : []);

const seedManagedAgentApiKeys = async () => {
  if (!ledgerPool || !CONFIGURED_AGENT_KEY_ENTRIES.length) return;
  for (const entry of CONFIGURED_AGENT_KEY_ENTRIES) {
    await ledgerPool.query(
      `
        insert into prediction_market_api_keys (
          agent_id,
          label,
          key_prefix,
          key_hash,
          scopes,
          collateral_usd,
          claimant_address,
          rate_limit_per_minute,
          status,
          source,
          updated_at
        )
        values ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,'active','config_seed', now())
        on conflict (key_hash) do update set
          agent_id = excluded.agent_id,
          label = excluded.label,
          key_prefix = excluded.key_prefix,
          scopes = excluded.scopes,
          collateral_usd = excluded.collateral_usd,
          claimant_address = excluded.claimant_address,
          rate_limit_per_minute = excluded.rate_limit_per_minute,
          status = 'active',
          source = 'config_seed',
          revoked_at = null,
          updated_at = now()
      `,
      [
        entry.record.agentId,
        entry.record.label,
        entry.rawKey.slice(0, 8),
        hashApiKey(entry.rawKey),
        JSON.stringify(scopeSetToArray(entry.record.scopes)),
        entry.record.collateralUsd,
        entry.record.claimantAddress,
        entry.record.rateLimitPerMinute,
      ],
    );
  }
};

const extractAuthToken = req => {
  const authorization = req.headers.get("authorization") ?? req.headers.get("Authorization");
  if (authorization) {
    const match = authorization.match(/^Bearer\s+(.+)$/i);
    if (match?.[1]) return { token: match[1].trim(), source: "authorization" };
  }
  const headerToken = req.headers.get("x-pm-agent-key");
  if (headerToken) return { token: headerToken.trim(), source: "x-pm-agent-key" };
  const url = new URL(req.url);
  const queryToken = url.searchParams.get("api_key");
  if (queryToken) return { token: queryToken.trim(), source: "query" };
  return null;
};

const recordAgentAuthUsage = authRecord => {
  if (!ledgerPool || !authRecord?.keyId) return;
  const lastWriteAt = authLastUsedWriteAt.get(authRecord.keyId) ?? 0;
  const nowMs = Date.now();
  if (nowMs - lastWriteAt < AUTH_LAST_USED_WRITE_TTL_MS) return;
  authLastUsedWriteAt.set(authRecord.keyId, nowMs);
  void ledgerPool
    .query(
      `
        update prediction_market_api_keys
        set last_used_at = now(),
            updated_at = now()
        where id = $1
      `,
      [authRecord.keyId],
    )
    .catch(error => {
      console.error("[prediction-markets] failed to update api key last_used_at", error);
    });
};

const purgeAuthCacheByKeyId = keyId => {
  for (const [cacheKey, entry] of authCache.entries()) {
    if (String(entry.record?.keyId ?? "") === String(keyId)) {
      authCache.delete(cacheKey);
    }
  }
};

const enforceAgentRateLimit = authRecord => {
  if (!authRecord?.rateLimitPerMinute) return { ok: true };
  const windowMs = 60 * 1000;
  const nowMs = Date.now();
  const key = String(authRecord.keyId ?? authRecord.agentId ?? "agent");
  const activeWindow = authRateWindows.get(key);
  if (!activeWindow || nowMs >= activeWindow.resetAtMs) {
    authRateWindows.set(key, { count: 1, resetAtMs: nowMs + windowMs });
    return { ok: true };
  }
  if (activeWindow.count >= authRecord.rateLimitPerMinute) {
    return agentError(
      "RATE_LIMIT_EXCEEDED",
      `Rate limit exceeded for agent key. Retry after ${Math.max(1, Math.ceil((activeWindow.resetAtMs - nowMs) / 1000))} seconds.`,
    );
  }
  activeWindow.count += 1;
  return { ok: true };
};

const requireAgentScope = (agent, scope) => {
  if (!agent) return agentError("AUTH_REQUIRED");
  if (agent.scopes?.has?.("*") || agent.scopes?.has?.("admin") || agent.scopes?.has?.(scope)) {
    return { ok: true };
  }
  return agentError("AUTH_SCOPE_MISSING", `Agent key is missing required scope: ${scope}.`);
};

const authenticateAgentRequest = async req => {
  const extracted = extractAuthToken(req);
  if (!extracted?.token) return null;
  const cacheKey = hashApiKey(extracted.token);
  const cached = authCache.get(cacheKey);
  if (cached && cached.expiresAtMs > Date.now()) {
    const limitCheck = enforceAgentRateLimit(cached.record);
    if (!limitCheck.ok) {
      return limitCheck;
    }
    recordAgentAuthUsage(cached.record);
    return { ok: true, agent: cached.record };
  }

  if (ledgerPool) {
    const { rows } = await ledgerPool.query(
      `
        select
          id,
          agent_id,
          label,
          key_prefix,
          scopes,
          collateral_usd,
          claimant_address,
          rate_limit_per_minute,
          source
        from prediction_market_api_keys
        where key_hash = $1
          and status = 'active'
        limit 1
      `,
      [cacheKey],
    );
    const row = rows[0] ?? null;
    if (row) {
      const record = normalizeAgentAuthRecord({
        id: String(row.id),
        keyId: String(row.id),
        keyPrefix: row.key_prefix,
        agentId: row.agent_id,
        label: row.label,
        collateralUsd: parseNullableNumber(row.collateral_usd),
        claimantAddress: row.claimant_address ?? null,
        scopes: normalizeScopeList(row.scopes),
        rateLimitPerMinute: parseNullableNumber(row.rate_limit_per_minute),
        source: row.source ?? "managed",
      });
      authCache.set(cacheKey, { record, expiresAtMs: Date.now() + AUTH_CACHE_TTL_MS });
      const limitCheck = enforceAgentRateLimit(record);
      if (!limitCheck.ok) {
        return limitCheck;
      }
      recordAgentAuthUsage(record);
      return { ok: true, agent: record };
    }
  }

  const fallbackAgent = DEV_AGENT_KEYS.get(extracted.token) ?? null;
  if (!fallbackAgent) return null;
  const limitCheck = enforceAgentRateLimit(fallbackAgent);
  if (!limitCheck.ok) {
    return limitCheck;
  }
  return { ok: true, agent: fallbackAgent };
};

const getAgentAuthRecord = async ({ agentId }) => {
  if (!agentId) return null;
  const managed = ledgerPool
    ? (
        await ledgerPool.query(
          `
            select
              id,
              agent_id,
              label,
              key_prefix,
              scopes,
              collateral_usd,
              claimant_address,
              rate_limit_per_minute,
              source
            from prediction_market_api_keys
            where agent_id = $1
              and status = 'active'
            order by id desc
            limit 1
          `,
          [agentId],
        )
      ).rows[0] ?? null
    : null;
  if (managed) {
    return normalizeAgentAuthRecord({
      id: String(managed.id),
      keyId: String(managed.id),
      keyPrefix: managed.key_prefix,
      agentId: managed.agent_id,
      label: managed.label,
      collateralUsd: parseNullableNumber(managed.collateral_usd),
      claimantAddress: managed.claimant_address ?? null,
      scopes: normalizeScopeList(managed.scopes),
      rateLimitPerMinute: parseNullableNumber(managed.rate_limit_per_minute),
      source: managed.source ?? "managed",
    });
  }
  for (const record of DEV_AGENT_KEYS.values()) {
    if (record.agentId === agentId) return record;
  }
  return null;
};

const createAbstractRelayClients = () => {
  if (abstractRelayClients) return abstractRelayClients;
  if (!ABSTRACT_TESTNET_RPC_URL || !NONCE_RELAY_PRIVATE_KEY) {
    throw new Error("abstract_testnet mode requires abstractTestnetRpcUrl and nonceRelayPrivateKey in config.");
  }
  const account = privateKeyToAccount(NONCE_RELAY_PRIVATE_KEY);
  abstractRelayClients = {
    publicClient: createPublicClient({
      chain: abstractTestnet,
      transport: http(ABSTRACT_TESTNET_RPC_URL),
    }),
    walletClient: createWalletClient({
      account,
      chain: abstractTestnet,
      transport: http(ABSTRACT_TESTNET_RPC_URL),
    }),
    account,
  };
  return abstractRelayClients;
};

const createNonceRelayClients = () => {
  if (nonceRelayClients) return nonceRelayClients;
  if (NONCE_RELAY_MODE !== "abstract_testnet") return null;
  if (!NONCE_MANAGER_ADDRESS) {
    throw new Error("abstract_testnet relay mode requires nonceManagerAddress in config.");
  }
  nonceRelayClients = createAbstractRelayClients();
  return nonceRelayClients;
};

const createSettlementRelayClients = () => {
  if (SETTLEMENT_MODE !== "abstract_testnet") return null;
  if (!SETTLEMENT_MANAGER_ADDRESS) {
    throw new Error("abstract_testnet settlement mode requires settlementManagerAddress in config.");
  }
  return createAbstractRelayClients();
};

const isVaultBalanceModeEnabled = () =>
  Boolean(ABSTRACT_TESTNET_RPC_URL && VAULT_ADDRESS && CLEARINGHOUSE_ADDRESS);

const resolveAgentChainAddress = agent => {
  const candidate =
    agent?.claimantAddress ??
    (typeof agent?.agentId === "string" && isAddress(agent.agentId) ? agent.agentId : null) ??
    null;
  return candidate && isAddress(candidate) ? getAddress(candidate) : null;
};

const readVaultAccountState = async userAddress => {
  if (!isVaultBalanceModeEnabled() || !userAddress || !isAddress(userAddress)) return null;
  const { publicClient } = createAbstractRelayClients();
  const stateResult = await publicClient.readContract({
    address: VAULT_ADDRESS,
    abi: VAULT_ABI,
    functionName: "getAccountState",
    args: [getAddress(userAddress)],
  });
  return {
    availableMicrousd: BigInt(stateResult.availableMicrousd ?? stateResult[0] ?? 0n),
    reservedMicrousd: BigInt(stateResult.reservedMicrousd ?? stateResult[1] ?? 0n),
    claimableMicrousd: BigInt(stateResult.claimableMicrousd ?? stateResult[2] ?? 0n),
    totalDepositedMicrousd: BigInt(stateResult.totalDepositedMicrousd ?? stateResult[3] ?? 0n),
    totalWithdrawnMicrousd: BigInt(stateResult.totalWithdrawnMicrousd ?? stateResult[4] ?? 0n),
  };
};

const readClearinghouseNetYesShares = async ({ marketId, traderAddress }) => {
  if (!isVaultBalanceModeEnabled() || !marketId || !traderAddress || !isAddress(traderAddress)) return null;
  const { publicClient } = createAbstractRelayClients();
  const marketKey = await publicClient.readContract({
    address: CLEARINGHOUSE_ADDRESS,
    abi: CLEARINGHOUSE_ABI,
    functionName: "marketKey",
    args: [String(marketId)],
  });
  const netYesMicroshares = await publicClient.readContract({
    address: CLEARINGHOUSE_ADDRESS,
    abi: CLEARINGHOUSE_ABI,
    functionName: "netYesMicroshares",
    args: [marketKey, getAddress(traderAddress)],
  });
  return microsharesToShares(netYesMicroshares);
};

const marketIdToContractKey = marketId => keccak256(stringToHex(String(marketId)));
const resultToResultCode = result => (result === "up" ? 1 : result === "down" ? 2 : 3);
const resultCodeToResult = resultCode => (Number(resultCode) === 1 ? "up" : Number(resultCode) === 2 ? "down" : "flat");
const usdToMicrousd = amountUsd => BigInt(Math.round(normalizeUsd(Number(amountUsd) || 0) * 1_000_000));
const shouldRunChainReconciliation = () =>
  Boolean(
    ledgerPool &&
      ABSTRACT_TESTNET_RPC_URL &&
      (NONCE_RELAY_MODE === "abstract_testnet" ||
        FILL_SETTLEMENT_MODE === "abstract_testnet" ||
        SETTLEMENT_MODE === "abstract_testnet"),
  );

const rowToRound = row => ({
  startMs: parseNullableNumber(row.start_ms),
  endMs: parseNullableNumber(row.end_ms),
  targetPrice: parseNullableNumber(row.target_price),
  currentPrice: parseNullableNumber(row.current_price),
  closePrice: parseNullableNumber(row.close_price),
  status: row.status,
  result: row.result,
  firstTradeTimeMs: parseNullableNumber(row.first_trade_time_ms),
  resolvedAtMs: parseNullableNumber(row.resolved_at_ms),
  resolutionTxHash: row.resolution_tx_hash ?? null,
  resolutionMode: row.resolution_mode ?? null,
});

const listResolvedRoundsPage = async ({ cursorStartMs = null, limit = RESOLUTION_PAGE_SIZE_DEFAULT } = {}) => {
  if (!ledgerPool) {
    return { items: state.resolvedRounds.slice(0, limit), nextCursor: null };
  }
  const safeLimit = Math.max(1, Math.min(RESOLUTION_PAGE_SIZE_MAX, Math.floor(limit)));
  const params = [LEDGER_VENUE, SYMBOL];
  let cursorClause = "";
  if (Number.isFinite(cursorStartMs)) {
    params.push(cursorStartMs);
    cursorClause = `and start_ms < $${params.length}`;
  }
  params.push(safeLimit + 1);
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_rounds
      where venue = $1
        and symbol = $2
        and status = 'resolved'
        ${cursorClause}
      order by start_ms desc
      limit $${params.length}
    `,
    params,
  );
  const mapped = rows.map(rowToRound);
  const items = mapped.slice(0, safeLimit);
  const nextCursor = mapped.length > safeLimit ? items[items.length - 1]?.startMs ?? null : null;
  return { items, nextCursor };
};

const getChainCursor = async stream => {
  if (!ledgerPool) return 0n;
  const { rows } = await ledgerPool.query(
    `
      select last_block_number
      from prediction_market_chain_cursors
      where stream = $1
      limit 1
    `,
    [stream],
  );
  return rows[0] ? BigInt(rows[0].last_block_number) : 0n;
};

const setChainCursor = async (stream, lastBlockNumber) => {
  if (!ledgerPool || !stream) return;
  await ledgerPool.query(
    `
      insert into prediction_market_chain_cursors (
        stream,
        last_block_number,
        updated_at
      )
      values ($1,$2, now())
      on conflict (stream) do update set
        last_block_number = excluded.last_block_number,
        updated_at = now()
    `,
    [stream, lastBlockNumber.toString()],
  );
};

const getBlockTimestampMs = async (publicClient, blockNumber) => {
  const cacheKey = String(blockNumber);
  if (blockTimestampCache.has(cacheKey)) {
    return blockTimestampCache.get(cacheKey);
  }
  const block = await publicClient.getBlock({ blockNumber });
  const timestampMs = Number(block.timestamp) * 1000;
  blockTimestampCache.set(cacheKey, timestampMs);
  return timestampMs;
};

const findOrderCandidatesForMakerNonce = async ({ makerAddress, nonce }) => {
  if (!ledgerPool) return [];
  const { rows } = await ledgerPool.query(
    `
      select order_hash, market_id, agent_id
      from prediction_market_orders
      where lower(maker_address) = lower($1)
        and nonce = $2
      order by accepted_at_ms desc
    `,
    [makerAddress, nonce],
  );
  return rows;
};

const findAgentIdByClaimantAddress = async claimantAddress => {
  if (!ledgerPool || !claimantAddress) return null;
  const { rows } = await ledgerPool.query(
    `
      select agent_id
      from prediction_market_api_keys
      where lower(claimant_address) = lower($1)
        and status = 'active'
      order by id desc
      limit 1
    `,
    [claimantAddress],
  );
  return rows[0]?.agent_id ?? null;
};

const findClaimByTxHash = async txHash => {
  if (!ledgerPool || !txHash) return null;
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_claims
      where tx_hash = $1
      limit 1
    `,
    [txHash],
  );
  return rows[0] ?? null;
};

const findFillByTxHash = async txHash => {
  if (!ledgerPool || !txHash) return null;
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_fills
      where tx_hash = $1
      limit 1
    `,
    [txHash],
  );
  return rows[0] ?? null;
};

const reconcileNonceInvalidationLogs = async ({ publicClient, fromBlock, toBlock }) => {
  if (!ledgerPool || NONCE_RELAY_MODE !== "abstract_testnet" || !NONCE_MANAGER_ADDRESS || toBlock < fromBlock) return;
  const event = NONCE_MANAGER_ABI.find(entry => entry.type === "event" && entry.name === "NonceInvalidated");
  if (!event) return;
  const logs = await publicClient.getLogs({
    address: NONCE_MANAGER_ADDRESS,
    event,
    fromBlock,
    toBlock,
  });
  for (const log of logs) {
    const makerAddress = log.args?.maker ? getAddress(log.args.maker) : null;
    const nonce = Number(log.args?.nonce ?? NaN);
    const marketKey = String(log.args?.marketId ?? "");
    if (!makerAddress || !Number.isFinite(nonce) || !marketKey) continue;
    const confirmedAtMs = await getBlockTimestampMs(publicClient, log.blockNumber);
    let existingInvalidation = null;
    const { rows } = await ledgerPool.query(
      `
        select *
        from prediction_market_nonce_invalidations
        where lower(maker_address) = lower($1)
          and nonce = $2
        order by id desc
        limit 1
      `,
      [makerAddress, nonce],
    );
    existingInvalidation = rows[0] ?? null;
    if (!existingInvalidation) {
      const candidates = await findOrderCandidatesForMakerNonce({ makerAddress, nonce });
      const matchingOrder = candidates.find(candidate => marketIdToContractKey(candidate.market_id) === marketKey) ?? null;
      if (!matchingOrder) continue;
      const insertResult = await ledgerPool.query(
        `
          insert into prediction_market_nonce_invalidations (
            market_id,
            maker_address,
            nonce,
            agent_id,
            status,
            relay_mode,
            tx_hash,
            submitted_at_ms,
            confirmed_at_ms,
            error_message
          )
          values ($1,$2,$3,$4,'confirmed_onchain','abstract_testnet',$5,$6,$6,null)
          returning *
        `,
          [matchingOrder.market_id, makerAddress, nonce, matchingOrder.agent_id ?? null, log.transactionHash, confirmedAtMs],
        );
      existingInvalidation = insertResult.rows[0] ?? null;
    } else {
      const updateResult = await ledgerPool.query(
        `
          update prediction_market_nonce_invalidations
          set status = 'confirmed_onchain',
              relay_mode = 'abstract_testnet',
              tx_hash = $2,
              confirmed_at_ms = $3,
              error_message = null
          where id = $1
          returning *
        `,
        [existingInvalidation.id, log.transactionHash, confirmedAtMs],
      );
      existingInvalidation = updateResult.rows[0] ?? existingInvalidation;
    }
    if (!existingInvalidation) continue;
    const cancelledOrderRows = await ledgerPool.query(
      `
        update prediction_market_orders
        set status = 'cancelled',
            remaining_shares = 0,
            updated_at = now()
        where market_id = $1
          and lower(maker_address) = lower($2)
          and nonce = $3
          and status = any($4::text[])
        returning *
      `,
      [existingInvalidation.market_id, makerAddress, nonce, [...DEFAULT_OPEN_ORDER_STATUSES]],
    );
    for (const row of cancelledOrderRows.rows) {
      await appendOrderEvent({
        orderHash: row.order_hash,
        marketId: row.market_id,
        eventType: "order.reconciled_cancel",
        payload: {
          order_hash: row.order_hash,
          market_id: row.market_id,
          nonce,
          tx_hash: log.transactionHash,
          reconciled_at_ms: confirmedAtMs,
        },
      });
      removeBookOrder({ market_id: row.market_id, order_hash: row.order_hash });
      if (row.agent_id) {
        await broadcastAgentPositionsDelta({ agentId: row.agent_id, marketId: row.market_id });
      }
      broadcastBookDelta({
        marketId: row.market_id,
        reason: "order.reconciled_cancel",
        order: {
          ...normalizeOpenOrder(row),
          status: "cancelled",
          remaining_shares: 0,
        },
        side: mapSideToBookKey(row.side),
        priceTenths: Number(row.price_tenths),
      });
      broadcastBook(row.market_id);
    }
    broadcastNonceInvalidationUpdate({
      id: Number(existingInvalidation.id),
      market_id: existingInvalidation.market_id,
      maker_address: existingInvalidation.maker_address,
      nonce: Number(existingInvalidation.nonce),
      agent_id: existingInvalidation.agent_id,
      status: existingInvalidation.status,
      relay_mode: existingInvalidation.relay_mode ?? "abstract_testnet",
      tx_hash: existingInvalidation.tx_hash ?? log.transactionHash,
      error_message: existingInvalidation.error_message ?? null,
      submitted_at_ms: parseNullableNumber(existingInvalidation.submitted_at_ms),
      confirmed_at_ms: parseNullableNumber(existingInvalidation.confirmed_at_ms) ?? confirmedAtMs,
      created_at_ms: Number(new Date(existingInvalidation.created_at).getTime()),
    });
  }
};

const reconcileFillLogs = async ({ publicClient, fromBlock, toBlock }) => {
  if (!ledgerPool || FILL_SETTLEMENT_MODE !== "abstract_testnet" || !FILL_MANAGER_ADDRESS || toBlock < fromBlock) return;
  const event = FILL_MANAGER_ABI.find(entry => entry.type === "event" && entry.name === "OrderFilled");
  if (!event) return;
  const logs = await publicClient.getLogs({
    address: FILL_MANAGER_ADDRESS,
    event,
    fromBlock,
    toBlock,
  });
  for (const log of logs) {
    const orderHash = log.args?.orderHash ? String(log.args.orderHash) : null;
    const marketId = typeof log.args?.marketId === "string" ? log.args.marketId : null;
    if (!orderHash || !marketId) continue;
    const blockTimestampMs = await getBlockTimestampMs(publicClient, log.blockNumber);
    const orderRow = await findOrderByHash(orderHash);
    const fillShares = microsharesToShares(log.args?.fillMicroshares ?? 0n);
    const totalFilledShares = microsharesToShares(log.args?.totalFilledMicroshares ?? 0n);
    const existingFill = await findFillByTxHash(log.transactionHash);
    if (!existingFill) {
      await persistFill({
        market_id: marketId,
        order_hash: orderHash,
        maker_address: log.args?.maker ? getAddress(log.args.maker) : orderRow?.maker_address ?? null,
        maker_agent_id: orderRow?.agent_id ?? null,
        taker_address: log.args?.taker ? getAddress(log.args.taker) : "unknown",
        taker_agent_id: null,
        side: String(log.args?.side ?? orderRow?.side ?? "buy_yes"),
        price_tenths: Number(log.args?.priceTenths ?? orderRow?.price_tenths ?? 0),
        fill_shares: fillShares,
        tx_hash: log.transactionHash,
      });
    }
    let updatedOrder = null;
    if (orderRow) {
      const remainingShares = Math.max(0, Number(orderRow.size_shares) - totalFilledShares);
      const nextStatus = remainingShares > 0 ? "partially_filled" : "filled";
      const updateResult = await ledgerPool.query(
        `
          update prediction_market_orders
          set remaining_shares = $2,
              status = $3,
              updated_at = now()
          where order_hash = $1
          returning *
        `,
        [orderHash, remainingShares, nextStatus],
      );
      updatedOrder = updateResult.rows[0] ?? null;
      await appendOrderEvent({
        orderHash,
        marketId,
        eventType: "order.reconciled_fill",
        payload: {
          order_hash: orderHash,
          market_id: marketId,
          tx_hash: log.transactionHash,
          fill_shares: fillShares,
          total_filled_shares: totalFilledShares,
          remaining_shares: remainingShares,
          status: nextStatus,
          reconciled_at_ms: blockTimestampMs,
        },
      });
      if (updatedOrder && DEFAULT_OPEN_ORDER_STATUSES.has(updatedOrder.status)) {
        upsertBookOrder(normalizeOpenOrder(updatedOrder));
      } else if (updatedOrder) {
        removeBookOrder({ market_id: updatedOrder.market_id, order_hash: updatedOrder.order_hash });
      }
      if (updatedOrder?.agent_id) {
        await broadcastAgentPositionsDelta({ agentId: updatedOrder.agent_id, marketId });
      }
    }
    broadcastBookDelta({
      marketId,
      reason: "order.reconciled_fill",
      order: updatedOrder ? normalizeOpenOrder(updatedOrder) : null,
      side: updatedOrder ? mapSideToBookKey(updatedOrder.side) : mapSideToBookKey(String(log.args?.side ?? "buy_yes")),
      priceTenths: updatedOrder ? Number(updatedOrder.price_tenths) : Number(log.args?.priceTenths ?? 0),
    });
    broadcastBook(marketId);
  }
};

const reconcileSettlementLogs = async ({ publicClient, fromBlock, toBlock }) => {
  if (!ledgerPool || SETTLEMENT_MODE !== "abstract_testnet" || !SETTLEMENT_MANAGER_ADDRESS || toBlock < fromBlock) return;
  const roundResolvedEvent = SETTLEMENT_MANAGER_ABI.find(entry => entry.type === "event" && entry.name === "RoundResolved");
  const claimRecordedEvent = SETTLEMENT_MANAGER_ABI.find(entry => entry.type === "event" && entry.name === "ClaimRecorded");
  if (roundResolvedEvent) {
    const logs = await publicClient.getLogs({
      address: SETTLEMENT_MANAGER_ADDRESS,
      event: roundResolvedEvent,
      fromBlock,
      toBlock,
    });
    for (const log of logs) {
      const marketId = typeof log.args?.marketId === "string" ? log.args.marketId : null;
      if (!marketId) continue;
      const startMs = parseIntegerOrNull(String(marketId).split("-").at(-1));
      if (!Number.isFinite(startMs)) continue;
      const resolvedAtMs = Number(log.args?.resolvedAtMs ?? Date.now());
      const result = resultCodeToResult(log.args?.resultCode ?? 3);
      await ledgerPool.query(
        `
          insert into prediction_market_rounds (
            venue,
            symbol,
            start_ms,
            end_ms,
            target_price,
            current_price,
            close_price,
            status,
            result,
            first_trade_time_ms,
            resolved_at_ms,
            resolution_tx_hash,
            resolution_mode,
            updated_at
          )
          values ($1,$2,$3,$4,null,null,null,'resolved',$5,null,$6,$7,'abstract_testnet',now())
          on conflict (venue, symbol, start_ms) do update set
            status = 'resolved',
            result = excluded.result,
            resolved_at_ms = excluded.resolved_at_ms,
            resolution_tx_hash = excluded.resolution_tx_hash,
            resolution_mode = excluded.resolution_mode,
            updated_at = now()
        `,
        [LEDGER_VENUE, SYMBOL, startMs, startMs + ROUND_MS, result, resolvedAtMs, log.transactionHash],
      );
    }
  }
  if (claimRecordedEvent) {
    const logs = await publicClient.getLogs({
      address: SETTLEMENT_MANAGER_ADDRESS,
      event: claimRecordedEvent,
      fromBlock,
      toBlock,
    });
    for (const log of logs) {
      const marketId = typeof log.args?.marketId === "string" ? log.args.marketId : null;
      const claimantAddress = log.args?.claimant ? getAddress(log.args.claimant) : null;
      if (!marketId || !claimantAddress) continue;
      const amountUsd = normalizeUsd(Number(log.args?.amountMicrousd ?? 0n) / 1_000_000);
      const claimTimestampMs = await getBlockTimestampMs(publicClient, log.blockNumber);
      let existingClaim = await findClaimByTxHash(log.transactionHash);
      let agentId = existingClaim?.agent_id ?? null;
      if (!agentId) {
        const { rows } = await ledgerPool.query(
          `
            select agent_id
            from prediction_market_claims
            where market_id = $1
              and lower(claimant_address) = lower($2)
            limit 1
          `,
          [marketId, claimantAddress],
        );
        agentId = rows[0]?.agent_id ?? null;
      }
      if (!agentId) {
        agentId = await findAgentIdByClaimantAddress(claimantAddress);
      }
      if (!agentId) {
        console.warn("[prediction-markets] unable to reconcile claim without mapped agent", {
          marketId,
          claimantAddress,
          txHash: log.transactionHash,
        });
        continue;
      }
      const roundRows = await ledgerPool.query(
        `
          select result
          from prediction_market_rounds
          where venue = $1
            and symbol = $2
            and start_ms = $3
          limit 1
        `,
        [LEDGER_VENUE, SYMBOL, parseIntegerOrNull(String(marketId).split("-").at(-1))],
      );
      const roundResult = roundRows.rows[0]?.result ?? "flat";
      if (existingClaim) {
        await ledgerPool.query(
          `
            update prediction_market_claims
            set claimant_address = $2,
                amount_usd = $3,
                round_result = $4,
                claim_mode = 'abstract_testnet',
                created_at_ms = $5
            where id = $1
          `,
          [existingClaim.id, claimantAddress, amountUsd, roundResult, claimTimestampMs],
        );
      } else {
        await ledgerPool.query(
          `
            insert into prediction_market_claims (
              market_id,
              agent_id,
              claimant_address,
              amount_usd,
              round_result,
              tx_hash,
              claim_mode,
              created_at_ms
            )
            values ($1,$2,$3,$4,$5,$6,'abstract_testnet',$7)
            on conflict (market_id, agent_id) do update set
              claimant_address = excluded.claimant_address,
              amount_usd = excluded.amount_usd,
              round_result = excluded.round_result,
              tx_hash = excluded.tx_hash,
              claim_mode = excluded.claim_mode,
              created_at_ms = excluded.created_at_ms
          `,
          [marketId, agentId, claimantAddress, amountUsd, roundResult, log.transactionHash, claimTimestampMs],
        );
      }
      await broadcastAgentPositionsDelta({ agentId, marketId });
    }
  }
};

const runChainReconciliationPass = async ({
  force = false,
  fromBlockOverride = null,
  toBlockOverride = null,
  updateCursors = true,
} = {}) => {
  if (!shouldRunChainReconciliation() || (!force && reconciliationRunning)) return;
  reconciliationRunning = true;
  state.monitoring.lastReconciliationStartedAtMs = Date.now();
  try {
    const { publicClient } = createAbstractRelayClients();
    const latestBlock = toBlockOverride ?? (await publicClient.getBlockNumber());
    if (NONCE_RELAY_MODE === "abstract_testnet" && NONCE_MANAGER_ADDRESS) {
      const cursor = await getChainCursor("nonce_invalidations");
      const fromBlock = fromBlockOverride ?? (cursor > 0n ? cursor + 1n : 0n);
      if (fromBlock <= latestBlock) {
        await reconcileNonceInvalidationLogs({ publicClient, fromBlock, toBlock: latestBlock });
      }
      if (updateCursors) {
        await setChainCursor("nonce_invalidations", latestBlock);
      }
    }
    if (FILL_SETTLEMENT_MODE === "abstract_testnet" && FILL_MANAGER_ADDRESS) {
      const cursor = await getChainCursor("fills");
      const fromBlock = fromBlockOverride ?? (cursor > 0n ? cursor + 1n : 0n);
      if (fromBlock <= latestBlock) {
        await reconcileFillLogs({ publicClient, fromBlock, toBlock: latestBlock });
      }
      if (updateCursors) {
        await setChainCursor("fills", latestBlock);
      }
    }
    if (SETTLEMENT_MODE === "abstract_testnet" && SETTLEMENT_MANAGER_ADDRESS) {
      const cursor = await getChainCursor("settlements");
      const fromBlock = fromBlockOverride ?? (cursor > 0n ? cursor + 1n : 0n);
      if (fromBlock <= latestBlock) {
        await reconcileSettlementLogs({ publicClient, fromBlock, toBlock: latestBlock });
      }
      if (updateCursors) {
        await setChainCursor("settlements", latestBlock);
      }
    }
    state.monitoring.lastReconciliationSuccessAtMs = Date.now();
    state.monitoring.lastReconciliationError = null;
  } catch (error) {
    state.monitoring.lastReconciliationFailureAtMs = Date.now();
    state.monitoring.lastReconciliationError = error instanceof Error ? error.message : String(error);
    console.error("[prediction-markets] chain reconciliation failed", error);
  } finally {
    reconciliationRunning = false;
  }
};

const ensureReconciliationTimer = () => {
  if (!shouldRunChainReconciliation()) return;
  if (reconciliationTimer) return;
  reconciliationTimer = setInterval(() => {
    void runChainReconciliationPass();
  }, RECONCILIATION_POLL_MS);
};

const ensureMonitoringTimer = () => {
  if (monitoringTimer) return;
  monitoringTimer = setInterval(() => {
    void processMonitoringTick();
  }, MONITORING_POLL_MS);
};

const persistRound = round => {
  if (!ledgerPool || !round?.startMs || !round?.endMs) return Promise.resolve();
  return enqueueLedgerWrite(async () => {
    await ledgerPool.query(
      `
        insert into prediction_market_rounds (
          venue,
          symbol,
          start_ms,
          end_ms,
          target_price,
          current_price,
          close_price,
          status,
        result,
        first_trade_time_ms,
        resolved_at_ms,
        resolution_tx_hash,
        resolution_mode,
        updated_at
      )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, now())
        on conflict (venue, symbol, start_ms) do update set
          end_ms = excluded.end_ms,
          target_price = excluded.target_price,
          current_price = excluded.current_price,
          close_price = excluded.close_price,
          status = excluded.status,
          result = excluded.result,
          first_trade_time_ms = excluded.first_trade_time_ms,
          resolved_at_ms = excluded.resolved_at_ms,
          resolution_tx_hash = excluded.resolution_tx_hash,
          resolution_mode = excluded.resolution_mode,
          updated_at = now()
      `,
      [
        LEDGER_VENUE,
        SYMBOL,
        round.startMs,
        round.endMs,
        round.targetPrice,
        round.currentPrice,
        round.closePrice ?? (round.status === "resolved" ? round.currentPrice : null),
        round.status,
        round.result,
        round.firstTradeTimeMs ?? null,
        round.resolvedAtMs ?? (round.status === "resolved" ? Date.now() : null),
        round.resolutionTxHash ?? null,
        round.resolutionMode ?? null,
      ],
    );
  });
};

const pruneResolvedRounds = () =>
  (!ledgerPool
    ? Promise.resolve()
    : enqueueLedgerWrite(async () => {
    await ledgerPool.query(
      `
        delete from prediction_market_rounds
        where venue = $1
          and symbol = $2
          and status = 'resolved'
          and start_ms not in (
            select start_ms
            from prediction_market_rounds
            where venue = $1
              and symbol = $2
              and status = 'resolved'
            order by start_ms desc
            limit $3
          )
      `,
      [LEDGER_VENUE, SYMBOL, MAX_RESOLVED_ROWS],
    );
    }));

const hydrateLedgerState = async () => {
  if (!ledgerPool) return;
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_rounds
      where venue = $1
        and symbol = $2
      order by start_ms desc
      limit $3
    `,
    [LEDGER_VENUE, SYMBOL, MAX_RESOLVED_ROWS + 2],
  );
  if (!rows.length) return;
  const mapped = rows.map(rowToRound);
  state.currentRound = mapped.find(entry => entry.status !== "resolved") ?? null;
  state.previousRound = mapped.find(entry => entry.status === "resolved") ?? null;
  state.resolvedRounds = mapped.filter(entry => entry.status === "resolved").slice(0, MAX_RESOLVED_ROWS);
  const activeMarketId = getCurrentMarketId(state.currentRound);
  if (!activeMarketId) return;
  const { rows: orderRows } = await ledgerPool.query(
    `
      select *
      from prediction_market_orders
      where market_id = $1
        and status = any($2::text[])
      order by accepted_at_ms asc
    `,
    [activeMarketId, [...DEFAULT_OPEN_ORDER_STATUSES]],
  );
  books.set(activeMarketId, buildEmptyBook(activeMarketId));
  for (const row of orderRows) {
    upsertBookOrder(normalizeOpenOrder(row));
  }
};

const hydrateReplayStreamState = async () => {
  if (!ledgerPool) return;
  const { rows } = await ledgerPool.query(
    `
      select stream_key, sequence, event_type, payload
      from (
        select
          stream_key,
          sequence,
          event_type,
          payload,
          row_number() over (partition by stream_key order by sequence desc) as row_num
        from prediction_market_stream_events
      ) ranked
      where row_num <= $1
      order by stream_key asc, sequence asc
    `,
    [WS_REPLAY_HISTORY_LIMIT],
  );
  const latestSequenceByBookMarket = new Map();
  agentStreamState.clear();
  for (const row of rows) {
    const streamKey = String(row.stream_key);
    const entry = appendAgentStreamEvent({
      streamKey,
      type: row.event_type,
      payload: row.payload ?? {},
      sequence: Number(row.sequence),
      persist: false,
    });
    if (streamKey.startsWith("book:")) {
      const [, , marketId] = streamKey.split(":");
      if (marketId) {
        latestSequenceByBookMarket.set(marketId, Math.max(latestSequenceByBookMarket.get(marketId) ?? 0, entry.sequence));
      }
    }
  }
  for (const [marketId, sequence] of latestSequenceByBookMarket.entries()) {
    const book = books.get(marketId);
    if (book) {
      book.sequence = Math.max(book.sequence ?? 0, sequence);
    } else {
      const nextBook = buildEmptyBook(marketId);
      nextBook.sequence = sequence;
      books.set(marketId, nextBook);
    }
  }
};

const roundStartFor = timeMs => Math.floor(timeMs / ROUND_MS) * ROUND_MS;
const roundStatusFor = (nowMs, round) => {
  if (!round) return "pending";
  if (nowMs < round.startMs) return "countdown";
  if (nowMs >= round.endMs) return "resolved";
  return "live";
};
const roundResultFor = round => {
  if (!round || !Number.isFinite(round.targetPrice) || !Number.isFinite(round.currentPrice)) return null;
  if (round.currentPrice > round.targetPrice) return "up";
  if (round.currentPrice < round.targetPrice) return "down";
  return "flat";
};

const buildRound = (timeMs, targetPrice, currentPrice) => {
  const startMs = roundStartFor(timeMs);
  const endMs = startMs + ROUND_MS;
  const round = {
    startMs,
    endMs,
    targetPrice: Number.isFinite(targetPrice) ? Number(targetPrice) : null,
    currentPrice: Number.isFinite(currentPrice) ? Number(currentPrice) : null,
    status: Number.isFinite(targetPrice) ? "live" : "waiting_for_open",
    result: null,
    firstTradeTimeMs: null,
    resolvedAtMs: null,
  };
  if (Number.isFinite(targetPrice)) {
    round.status = roundStatusFor(timeMs, round);
  }
  round.result = roundResultFor(round);
  return round;
};

const buildBootstrapPayload = () => ({
  type: "pm.bootstrap",
  payload: {
    symbol: SYMBOL,
    feedConnected: state.feedConnected,
    serverNowMs: Date.now(),
    lastTrade: state.lastTrade,
    lastPrice: state.lastPrice,
    quote: state.quote,
    series: state.chartSeries,
    currentRound: state.currentRound,
    previousRound: state.previousRound,
    resolvedRounds: state.resolvedRounds,
  },
});

const buildAgentBootstrapPayload = ws => {
  const marketId = getCurrentMarketId(state.currentRound);
  const bookStreamKey = marketId ? buildStreamKey({ kind: "book", marketId }) : null;
  const positionsStreamKey = buildStreamKey({ kind: "positions", marketId, agentId: ws.data?.agent?.agentId ?? null });
  const nonceStreamKey = buildStreamKey({ kind: "nonce_invalidations", marketId, agentId: ws.data?.agent?.agentId ?? null });
  return {
    type: "agent.bootstrap",
    payload: {
      server_now_ms: Date.now(),
      agent_id: ws.data?.agent?.agentId ?? null,
      market_id: marketId,
      protocol_version: AGENT_PROTOCOL_VERSION,
      current_round: state.currentRound,
      book: marketId ? serializeBook(marketId) : null,
      positions: [],
      feed_connected: state.feedConnected,
      last_price: state.lastPrice,
      quote: state.quote,
      auth: {
        key_id: ws.data?.agent?.keyId ?? null,
        key_prefix: ws.data?.agent?.keyPrefix ?? null,
        source: ws.data?.agent?.source ?? null,
        scopes: ws.data?.agent?.scopes ? scopeSetToArray(ws.data.agent.scopes) : [],
        rate_limit_per_minute: ws.data?.agent?.rateLimitPerMinute ?? null,
      },
      streams: {
        book:
          bookStreamKey === null
            ? null
            : {
                stream: bookStreamKey,
                current_sequence: (books.get(marketId)?.sequence ?? 0),
                oldest_replayable_sequence: streamOldestReplayableSequence(bookStreamKey),
              },
        positions: {
          stream: positionsStreamKey,
          current_sequence: ensureAgentStream(positionsStreamKey).sequence,
          oldest_replayable_sequence: streamOldestReplayableSequence(positionsStreamKey),
        },
        nonce_invalidations: {
          stream: nonceStreamKey,
          current_sequence: ensureAgentStream(nonceStreamKey).sequence,
          oldest_replayable_sequence: streamOldestReplayableSequence(nonceStreamKey),
        },
      },
      capabilities: {
        order_place: ws.data?.agent?.scopes?.has?.("*") || ws.data?.agent?.scopes?.has?.("admin") || ws.data?.agent?.scopes?.has?.("orders:write"),
        order_cancel: ws.data?.agent?.scopes?.has?.("*") || ws.data?.agent?.scopes?.has?.("admin") || ws.data?.agent?.scopes?.has?.("orders:cancel"),
        order_take: ws.data?.agent?.scopes?.has?.("*") || ws.data?.agent?.scopes?.has?.("admin") || ws.data?.agent?.scopes?.has?.("fills:take"),
        fills_onchain: FILL_SETTLEMENT_MODE === "abstract_testnet",
        claims_onchain: SETTLEMENT_MODE === "abstract_testnet",
        nonce_relay_mode: NONCE_RELAY_MODE,
        settlement_mode: SETTLEMENT_MODE,
      },
    },
  };
};

const sendAgentPositionsSnapshot = async (ws, requestedMarketId = null) => {
  if (!ws?.data?.agent?.agentId) return;
  const marketId = requestedMarketId ?? getCurrentMarketId(state.currentRound);
  const streamKey = buildStreamKey({ kind: "positions", marketId, agentId: ws.data.agent.agentId });
  const stream = ensureAgentStream(streamKey);
  const items = await listAgentPositions({
    agentId: ws.data.agent.agentId,
    marketId,
  });
  ws.send(
    JSON.stringify({
      type: "positions.snapshot",
      payload: {
        agent_id: ws.data.agent.agentId,
        market_id: marketId,
        items,
        server_now_ms: Date.now(),
        recovery: {
          stream: streamKey,
          sequence: stream.sequence,
          snapshot: true,
          oldest_replayable_sequence: streamOldestReplayableSequence(streamKey),
        },
      },
    }),
  );
};

const sendAgentNonceInvalidationsSnapshot = async (ws, requestedMarketId = null) => {
  if (!ws?.data?.agent?.agentId) return;
  const marketId = requestedMarketId ?? getCurrentMarketId(state.currentRound);
  const streamKey = buildStreamKey({ kind: "nonce_invalidations", marketId, agentId: ws.data.agent.agentId });
  const stream = ensureAgentStream(streamKey);
  const items = await listAgentNonceInvalidations({
    agentId: ws.data.agent.agentId,
    marketId,
  });
  ws.send(
    JSON.stringify({
      type: "nonce_invalidation.snapshot",
      payload: {
        agent_id: ws.data.agent.agentId,
        market_id: marketId,
        items,
        server_now_ms: Date.now(),
        recovery: {
          stream: streamKey,
          sequence: stream.sequence,
          snapshot: true,
          oldest_replayable_sequence: streamOldestReplayableSequence(streamKey),
        },
      },
    }),
  );
};

const broadcastAgentPositionsDelta = async ({ agentId, marketId }) => {
  const items = await listAgentPositions({ agentId, marketId });
  const streamKey = buildStreamKey({ kind: "positions", marketId, agentId });
  const entry = appendAgentStreamEvent({
    streamKey,
    type: "positions.delta",
    payload: {
      agent_id: agentId,
      market_id: marketId,
      items,
      server_now_ms: Date.now(),
    },
  });
  broadcastAgent(
    {
      type: entry.type,
      payload: {
        ...entry.payload,
        recovery: {
          stream: streamKey,
          sequence: entry.sequence,
          previous_sequence: entry.sequence - 1,
          replayed: false,
        },
      },
    },
    client => client.data?.agent?.agentId === agentId,
  );
};

const sendStreamSync = (ws, { stream, mode, fromSequence = null, toSequence = null }) => {
  ws.send(
    JSON.stringify({
      type: "stream.sync",
      payload: {
        stream,
        mode,
        from_sequence: fromSequence,
        to_sequence: toSequence,
        server_now_ms: Date.now(),
      },
    }),
  );
};

const sendRecoveryRequired = (ws, { stream, requestedSequence, latestSequence, oldestReplayableSequence, reason }) => {
  ws.send(
    JSON.stringify({
      type: "stream.recovery_required",
      payload: {
        stream,
        requested_sequence: requestedSequence,
        latest_sequence: latestSequence,
        oldest_replayable_sequence: oldestReplayableSequence,
        reason,
        server_now_ms: Date.now(),
      },
    }),
  );
};

const sendReplayEvent = (ws, { streamKey, entry }) => {
  ws.send(
    JSON.stringify({
      type: entry.type,
      payload: {
        ...entry.payload,
        recovery: {
          stream: streamKey,
          sequence: entry.sequence,
          previous_sequence: Math.max(0, entry.sequence - 1),
          replayed: true,
          oldest_replayable_sequence: streamOldestReplayableSequence(streamKey),
        },
      },
    }),
  );
};

const handleReplayableSubscription = async ({
  ws,
  streamKey,
  sinceSequence,
  latestSequence = null,
  sendSnapshot,
  replayEvents,
}) => {
  const currentSequence = latestSequence ?? ensureAgentStream(streamKey).sequence;
  const normalizedSince = parseIntegerOrNull(sinceSequence);
  if (!Number.isInteger(normalizedSince)) {
    await sendSnapshot();
    sendStreamSync(ws, {
      stream: streamKey,
      mode: "snapshot",
      fromSequence: null,
      toSequence: currentSequence,
    });
    return;
  }
  const oldestReplayableSequence = streamOldestReplayableSequence(streamKey);
  if (normalizedSince > currentSequence) {
    sendRecoveryRequired(ws, {
      stream: streamKey,
      requestedSequence: normalizedSince,
      latestSequence: currentSequence,
      oldestReplayableSequence,
      reason: "future_sequence",
    });
    await sendSnapshot();
    sendStreamSync(ws, {
      stream: streamKey,
      mode: "snapshot_after_gap",
      fromSequence: normalizedSince,
      toSequence: currentSequence,
    });
    return;
  }
  if (normalizedSince < oldestReplayableSequence) {
    sendRecoveryRequired(ws, {
      stream: streamKey,
      requestedSequence: normalizedSince,
      latestSequence: currentSequence,
      oldestReplayableSequence,
      reason: "gap_out_of_range",
    });
    await sendSnapshot();
    sendStreamSync(ws, {
      stream: streamKey,
      mode: "snapshot_after_gap",
      fromSequence: normalizedSince,
      toSequence: currentSequence,
    });
    return;
  }
  const events = replayEvents(normalizedSince);
  for (const entry of events) {
    sendReplayEvent(ws, { streamKey, entry });
  }
  sendStreamSync(ws, {
    stream: streamKey,
    mode: "replay",
    fromSequence: normalizedSince,
    toSequence: currentSequence,
  });
};

const subscribeBookStream = async ({ ws, marketId, sinceSequence = null }) => {
  if (!marketId) {
    ws.send(JSON.stringify({ type: "book.reject", payload: { error: "No active market." } }));
    return;
  }
  ws.data.subscriptions.books.add(marketId);
  const streamKey = buildStreamKey({ kind: "book", marketId });
  await handleReplayableSubscription({
    ws,
    streamKey,
    sinceSequence,
    latestSequence: serializeBook(marketId).sequence,
    sendSnapshot: async () => {
      ws.send(JSON.stringify({ type: "book.snapshot", payload: serializeBook(marketId) }));
    },
    replayEvents: normalizedSince => replayAgentStreamEvents({ streamKey, sinceSequence: normalizedSince }),
  });
};

const subscribePositionsStream = async ({ ws, marketId, sinceSequence = null }) => {
  const streamKey = buildStreamKey({ kind: "positions", marketId, agentId: ws.data.agent.agentId });
  await handleReplayableSubscription({
    ws,
    streamKey,
    sinceSequence,
    sendSnapshot: async () => sendAgentPositionsSnapshot(ws, marketId),
    replayEvents: normalizedSince => replayAgentStreamEvents({ streamKey, sinceSequence: normalizedSince }),
  });
};

const subscribeNonceInvalidationsStream = async ({ ws, marketId, sinceSequence = null }) => {
  const streamKey = buildStreamKey({ kind: "nonce_invalidations", marketId, agentId: ws.data.agent.agentId });
  await handleReplayableSubscription({
    ws,
    streamKey,
    sinceSequence,
    sendSnapshot: async () => sendAgentNonceInvalidationsSnapshot(ws, marketId),
    replayEvents: normalizedSince => replayAgentStreamEvents({ streamKey, sinceSequence: normalizedSince }),
  });
};

const listAgentNonceInvalidations = async ({ agentId, marketId = null, limit = ORDER_PAGE_SIZE_DEFAULT }) => {
  if (!ledgerPool) return [];
  const safeLimit = Math.max(1, Math.min(ORDER_PAGE_SIZE_MAX, Math.floor(limit)));
  const params = [agentId];
  let marketClause = "";
  if (marketId) {
    params.push(marketId);
    marketClause = `and market_id = $${params.length}`;
  }
  params.push(safeLimit);
  const { rows } = await ledgerPool.query(
    `
      select
        id,
        market_id,
        maker_address,
        nonce,
        agent_id,
        status,
        relay_mode,
        tx_hash,
        error_message,
        submitted_at_ms,
        confirmed_at_ms,
        extract(epoch from created_at) * 1000 as created_at_ms
      from prediction_market_nonce_invalidations
      where agent_id = $1
        ${marketClause}
      order by id desc
      limit $${params.length}
    `,
    params,
  );
  return rows.map(row => ({
    id: Number(row.id),
    market_id: row.market_id,
    maker_address: row.maker_address,
    nonce: Number(row.nonce),
    agent_id: row.agent_id,
    status: row.status,
    relay_mode: row.relay_mode,
    tx_hash: row.tx_hash,
    error_message: row.error_message,
    submitted_at_ms: parseNullableNumber(row.submitted_at_ms),
    confirmed_at_ms: parseNullableNumber(row.confirmed_at_ms),
    created_at_ms: Number(row.created_at_ms),
  }));
};

const serializeAgentApiKeyRow = row => ({
  id: String(row.id),
  agent_id: row.agent_id,
  label: row.label,
  key_prefix: row.key_prefix,
  scopes: normalizeScopeList(row.scopes),
  collateral_usd: parseOptionalFiniteNumber(row.collateral_usd),
  rate_limit_per_minute: parseOptionalFiniteNumber(row.rate_limit_per_minute),
  status: row.status,
  source: row.source,
  last_used_at_ms: row.last_used_at ? Number(new Date(row.last_used_at).getTime()) : null,
  revoked_at_ms: row.revoked_at ? Number(new Date(row.revoked_at).getTime()) : null,
  created_at_ms: row.created_at ? Number(new Date(row.created_at).getTime()) : null,
  updated_at_ms: row.updated_at ? Number(new Date(row.updated_at).getTime()) : null,
});

const listManagedAgentApiKeys = async ({ agentId = null, includeInactive = false } = {}) => {
  if (!ledgerPool) return [];
  const params = [];
  const clauses = [];
  if (agentId) {
    params.push(agentId);
    clauses.push(`agent_id = $${params.length}`);
  }
  if (!includeInactive) {
    clauses.push(`status = 'active'`);
  }
  const whereClause = clauses.length ? `where ${clauses.join(" and ")}` : "";
  const { rows } = await ledgerPool.query(
    `
      select
        id,
        agent_id,
        label,
        key_prefix,
        scopes,
        collateral_usd,
        rate_limit_per_minute,
        status,
        source,
        last_used_at,
        revoked_at,
        created_at,
        updated_at
      from prediction_market_api_keys
      ${whereClause}
      order by id desc
    `,
    params,
  );
  return rows.map(serializeAgentApiKeyRow);
};

const createManagedAgentApiKey = async ({
  agentId,
  label,
  scopes,
  collateralUsd = null,
  rateLimitPerMinute = null,
}) => {
  if (!ledgerPool) return { ok: false, error: "Ledger is not configured." };
  if (!agentId) return { ok: false, error: "agent_id is required." };
  if (!label) return { ok: false, error: "label is required." };
  const normalizedScopes = normalizeScopeList(scopes);
  if (!normalizedScopes.length) {
    return { ok: false, error: "scopes must contain at least one scope." };
  }
  const rawKey = `pmk_${crypto.randomUUID().replace(/-/g, "")}`;
  const { rows } = await ledgerPool.query(
    `
      insert into prediction_market_api_keys (
        agent_id,
        label,
        key_prefix,
        key_hash,
        scopes,
        collateral_usd,
        rate_limit_per_minute,
        status,
        source,
        updated_at
      )
      values ($1,$2,$3,$4,$5::jsonb,$6,$7,'active','managed', now())
      returning
        id,
        agent_id,
        label,
        key_prefix,
        scopes,
        collateral_usd,
        rate_limit_per_minute,
        status,
        source,
        last_used_at,
        revoked_at,
        created_at,
        updated_at
    `,
    [
      agentId,
      label,
      rawKey.slice(0, 8),
      hashApiKey(rawKey),
      JSON.stringify(normalizedScopes),
      parseOptionalFiniteNumber(collateralUsd),
      parseOptionalFiniteNumber(rateLimitPerMinute),
    ],
  );
  return {
    ok: true,
    api_key: rawKey,
    item: serializeAgentApiKeyRow(rows[0]),
  };
};

const updateManagedAgentApiKey = async ({
  keyId,
  label,
  scopes,
  collateralUsd,
  rateLimitPerMinute,
  status,
}) => {
  if (!ledgerPool) return { ok: false, error: "Ledger is not configured." };
  if (!keyId) return { ok: false, error: "key_id is required." };
  const patches = [];
  const params = [];
  if (label !== undefined) {
    params.push(String(label));
    patches.push(`label = $${params.length}`);
  }
  if (scopes !== undefined) {
    const normalizedScopes = normalizeScopeList(scopes);
    if (!normalizedScopes.length) {
      return { ok: false, error: "scopes must contain at least one scope." };
    }
    params.push(JSON.stringify(normalizedScopes));
    patches.push(`scopes = $${params.length}::jsonb`);
  }
  if (collateralUsd !== undefined) {
    params.push(parseOptionalFiniteNumber(collateralUsd));
    patches.push(`collateral_usd = $${params.length}`);
  }
  if (rateLimitPerMinute !== undefined) {
    params.push(parseOptionalFiniteNumber(rateLimitPerMinute));
    patches.push(`rate_limit_per_minute = $${params.length}`);
  }
  if (status !== undefined) {
    if (!["active", "inactive", "revoked"].includes(String(status))) {
      return { ok: false, error: "status must be active, inactive, or revoked." };
    }
    params.push(String(status));
    patches.push(`status = $${params.length}`);
    patches.push(`revoked_at = ${String(status) === "revoked" ? "now()" : "null"}`);
  }
  if (!patches.length) {
    return { ok: false, error: "No fields provided to update." };
  }
  params.push(String(keyId));
  const { rows } = await ledgerPool.query(
    `
      update prediction_market_api_keys
      set ${patches.join(", ")},
          updated_at = now()
      where id = $${params.length}
      returning
        id,
        agent_id,
        label,
        key_prefix,
        scopes,
        collateral_usd,
        rate_limit_per_minute,
        status,
        source,
        last_used_at,
        revoked_at,
        created_at,
        updated_at
    `,
    params,
  );
  if (!rows[0]) return { ok: false, error: "API key not found." };
  purgeAuthCacheByKeyId(keyId);
  authLastUsedWriteAt.delete(String(keyId));
  return { ok: true, item: serializeAgentApiKeyRow(rows[0]) };
};

const revokeManagedAgentApiKey = async keyId =>
  updateManagedAgentApiKey({
    keyId,
    status: "revoked",
  });

const appendOrderEvent = async ({ orderHash, marketId, eventType, payload }) => {
  if (!ledgerPool) return;
  await ledgerPool.query(
    `
      insert into prediction_market_order_events (
        order_hash,
        market_id,
        event_type,
        payload
      )
      values ($1,$2,$3,$4::jsonb)
    `,
    [orderHash, marketId, eventType, JSON.stringify(payload)],
  );
};

const persistOrder = async order => {
  if (!ledgerPool) return;
  await ledgerPool.query(
    `
      insert into prediction_market_orders (
        order_hash,
        market_id,
        maker_address,
        agent_id,
        side,
        price_tenths,
        size_shares,
      remaining_shares,
      nonce,
      expiry_ms,
      client_order_id,
      order_kind,
      signature,
      status,
      created_at_ms,
      accepted_at_ms,
      updated_at
    )
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16, now())
      on conflict (order_hash) do update set
        remaining_shares = excluded.remaining_shares,
        status = excluded.status,
        updated_at = now()
    `,
    [
      order.order_hash,
      order.market_id,
      order.maker_address,
      order.agent_id,
      order.side,
      order.price_tenths,
      order.size_shares,
      order.remaining_shares,
      order.nonce,
      order.expiry_ms,
      order.client_order_id ?? null,
      order.order_kind ?? "unsigned",
      order.signature ?? null,
      order.status,
      order.created_at_ms,
      order.accepted_at_ms,
    ],
  );
};

const findNonceInvalidation = async ({ makerAddress, nonce }) => {
  if (!ledgerPool) return null;
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_nonce_invalidations
      where lower(maker_address) = lower($1)
        and nonce = $2
        and status <> 'failed_onchain'
      order by id desc
      limit 1
    `,
    [makerAddress, nonce],
  );
  return rows[0] ?? null;
};

const findOrderByHash = async orderHash => {
  if (!ledgerPool) return null;
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_orders
      where order_hash = $1
      limit 1
    `,
    [orderHash],
  );
  return rows[0] ?? null;
};

const expireOrdersForMarket = async marketId => {
  if (!ledgerPool || !marketId) return [];
  const { rows } = await ledgerPool.query(
    `
      update prediction_market_orders
      set status = 'expired',
          remaining_shares = 0,
          updated_at = now()
      where market_id = $1
        and status = any($2::text[])
      returning *
    `,
    [marketId, [...DEFAULT_OPEN_ORDER_STATUSES]],
  );
  return rows.map(normalizeOpenOrder).map(order => ({
    ...order,
    status: "expired",
    remaining_shares: 0,
  }));
};

const getAgentReservedCollateralUsd = async agentId => {
  if (!ledgerPool) return 0;
  const { rows } = await ledgerPool.query(
    `
      select
        coalesce(
          sum(
            case
              when side = 'buy_yes' then remaining_shares * (price_tenths::numeric / 1000.0)
              when side = 'sell_yes' then remaining_shares * ((1000 - price_tenths)::numeric / 1000.0)
              else 0
            end
          ),
          0
        ) as reserved_collateral_usd
      from prediction_market_orders
      where agent_id = $1
        and status = any($2::text[])
    `,
    [agentId, [...DEFAULT_OPEN_ORDER_STATUSES]],
  );
  return normalizeUsd(Number(rows[0]?.reserved_collateral_usd ?? 0));
};

const getAgentCollateralCapacity = async agent => {
  if (!agent) {
    return {
      source: "none",
      totalUsd: null,
      reservedUsd: 0,
      effectiveAvailableUsd: null,
    };
  }
  const reservedUsd = await getAgentReservedCollateralUsd(agent.agentId);
  const chainAddress = resolveAgentChainAddress(agent);
  if (chainAddress && isVaultBalanceModeEnabled()) {
    const vaultState = await readVaultAccountState(chainAddress);
    if (vaultState) {
      const totalUsd = microusdToUsd(vaultState.availableMicrousd);
      return {
        source: "vault",
        totalUsd,
        reservedUsd,
        effectiveAvailableUsd: normalizeUsd(Math.max(0, totalUsd - reservedUsd)),
        chainAddress,
        vaultState,
      };
    }
  }
  if (Number.isFinite(agent.collateralUsd)) {
    return {
      source: "config",
      totalUsd: normalizeUsd(agent.collateralUsd),
      reservedUsd,
      effectiveAvailableUsd: normalizeUsd(Math.max(0, agent.collateralUsd - reservedUsd)),
      chainAddress: null,
      vaultState: null,
    };
  }
  return {
    source: "none",
    totalUsd: null,
    reservedUsd,
    effectiveAvailableUsd: null,
    chainAddress,
    vaultState: null,
  };
};

const persistFill = async fill => {
  if (!ledgerPool) return;
  await ledgerPool.query(
    `
      insert into prediction_market_fills (
        market_id,
        order_hash,
        maker_address,
        maker_agent_id,
        taker_address,
        taker_agent_id,
        side,
        price_tenths,
        fill_shares,
        tx_hash
      )
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `,
    [
      fill.market_id,
      fill.order_hash,
      fill.maker_address,
      fill.maker_agent_id ?? null,
      fill.taker_address,
      fill.taker_agent_id ?? null,
      fill.side,
      fill.price_tenths,
      fill.fill_shares,
      fill.tx_hash ?? null,
    ],
  );
};

const placeOrder = async ({ agent, payload }) => {
  const marketId = getCurrentMarketId(state.currentRound);
  if (!marketId) {
    return agentError("MARKET_NOT_ACTIVE");
  }
  if (!state.currentRound || state.currentRound.status !== "live") {
    return agentError("MARKET_NOT_LIVE");
  }
  const side = payload?.side;
  if (side !== "buy_yes" && side !== "sell_yes") {
    return agentError("INVALID_SIDE");
  }
  const priceTenths = Number(payload?.price_tenths);
  if (!Number.isInteger(priceTenths) || priceTenths < PRICE_TENTHS_MIN || priceTenths > PRICE_TENTHS_MAX) {
    return agentError("INVALID_PRICE_TENTHS");
  }
  const sizeShares = Number(payload?.size_shares);
  if (!Number.isFinite(sizeShares) || sizeShares <= 0) {
    return agentError("INVALID_SIZE_SHARES");
  }
  const nowMs = Date.now();
  const requestedMarketId = typeof payload?.market_id === "string" && payload.market_id ? payload.market_id : marketId;
  if (requestedMarketId !== marketId) {
    return agentError("MARKET_ID_MISMATCH");
  }
  const expiryMs = Number(payload?.expiry_ms ?? (state.currentRound.endMs + DEFAULT_ORDER_EXPIRY_BUFFER_MS));
  if (!Number.isFinite(expiryMs) || expiryMs <= nowMs) {
    return agentError("INVALID_EXPIRY_MS");
  }
  const makerAddress = typeof payload?.maker_address === "string" && payload.maker_address ? payload.maker_address : `agent:${agent.agentId}`;
  const nonce = Number.isFinite(Number(payload?.nonce)) ? Number(payload.nonce) : nowMs;
  const signature = typeof payload?.signature === "string" && payload.signature ? payload.signature : null;
  const requestedCollateralUsd = computeOrderCollateralUsd({
    side,
    priceTenths,
    remainingShares: Number(sizeShares.toFixed(8)),
  });
  const collateralCapacity = await getAgentCollateralCapacity(agent);
  if (Number.isFinite(collateralCapacity.totalUsd)) {
    if (collateralCapacity.reservedUsd + requestedCollateralUsd > collateralCapacity.totalUsd + 1e-9) {
      return {
        ...agentError(
          "INSUFFICIENT_COLLATERAL",
          `Insufficient collateral. Required ${requestedCollateralUsd.toFixed(3)} USD, available ${collateralCapacity.effectiveAvailableUsd.toFixed(3)} USD.`,
        ),
      };
    }
  }
  let orderHash = createOrderHash();
  let orderKind = "unsigned";
  if (signature) {
    if (!isAddress(makerAddress)) {
      return agentError("INVALID_MAKER_ADDRESS");
    }
    const signedMessage = normalizePredictionMarketOrderMessage({
      market_id: requestedMarketId,
      maker_address: makerAddress,
      side,
      price_tenths: priceTenths,
      size_shares: sizeShares,
      nonce,
      expiry_ms: expiryMs,
    });
    let recoveredAddress;
    try {
      recoveredAddress = await recoverPredictionMarketOrderSigner({
        message: signedMessage,
        signature,
        chainId: ORDER_SIGNING_CHAIN_ID,
        verifyingContract: ORDER_SIGNING_VERIFYING_CONTRACT,
      });
    } catch {
      return agentError("INVALID_SIGNATURE");
    }
    if (getAddress(recoveredAddress) !== getAddress(makerAddress)) {
      return agentError("SIGNATURE_MISMATCH");
    }
    const invalidation = await findNonceInvalidation({ makerAddress, nonce });
    if (invalidation) {
      return agentError("NONCE_ALREADY_INVALIDATED");
    }
    orderHash = hashPredictionMarketOrder({
      message: signedMessage,
      chainId: ORDER_SIGNING_CHAIN_ID,
      verifyingContract: ORDER_SIGNING_VERIFYING_CONTRACT,
    });
    orderKind = "signed";
  }
  const existingOrder = await findOrderByHash(orderHash);
  if (existingOrder) {
    return agentError("ORDER_HASH_EXISTS");
  }
  const order = {
    order_hash: orderHash,
    market_id: requestedMarketId,
    maker_address: signature ? getAddress(makerAddress) : makerAddress,
    agent_id: agent.agentId,
    side,
    price_tenths: priceTenths,
    size_shares: Number(sizeShares.toFixed(8)),
    remaining_shares: Number(sizeShares.toFixed(8)),
    nonce,
    expiry_ms: expiryMs,
    client_order_id: payload?.client_order_id ?? null,
    order_kind: orderKind,
    signature,
    status: "open",
    created_at_ms: nowMs,
    accepted_at_ms: nowMs,
  };
  await enqueueLedgerWrite(async () => {
    await persistOrder(order);
    await appendOrderEvent({
      orderHash: order.order_hash,
      marketId,
      eventType: "order.accepted",
      payload: order,
    });
  });
  upsertBookOrder(order);
  broadcastAgent(
    {
      type: "order.accepted",
      payload: {
        order,
        market_id: marketId,
        server_now_ms: Date.now(),
      },
    },
    client => client.data?.agent?.agentId === agent.agentId,
  );
  void emitAgentNotifications({
    agentId: agent.agentId,
    eventType: "order.accepted",
    payload: {
      order,
      market_id: marketId,
    },
  });
  await broadcastAgentPositionsDelta({ agentId: agent.agentId, marketId });
  broadcastBookDelta({ marketId, reason: "order.accepted", order });
  broadcastBook(marketId);
  return { ok: true, order };
};

const cancelOrder = async ({ agent, orderHash }) => {
  if (!orderHash) return agentError("ORDER_HASH_REQUIRED");
  if (!ledgerPool) return agentError("LEDGER_UNAVAILABLE");
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_orders
      where order_hash = $1
      limit 1
    `,
    [orderHash],
  );
  const row = rows[0];
  if (!row) return agentError("ORDER_NOT_FOUND");
  if (row.agent_id && row.agent_id !== agent.agentId) {
    return agentError("ORDER_NOT_OWNED_BY_AGENT");
  }
  if (!DEFAULT_OPEN_ORDER_STATUSES.has(row.status)) {
    return agentError("ORDER_NOT_OPEN", `Order is already ${row.status}.`);
  }
  await enqueueLedgerWrite(async () => {
    await ledgerPool.query(
      `
        update prediction_market_orders
        set status = 'cancelled',
            updated_at = now()
        where order_hash = $1
      `,
      [orderHash],
    );
    await ledgerPool.query(
      `
        insert into prediction_market_nonce_invalidations (
          market_id,
          maker_address,
          nonce,
          agent_id,
          status,
          relay_mode
        )
        values ($1,$2,$3,$4,$5,$6)
      `,
      [row.market_id, row.maker_address, row.nonce, agent.agentId, "pending_onchain", NONCE_RELAY_MODE],
    );
    await appendOrderEvent({
      orderHash,
      marketId: row.market_id,
      eventType: "order.cancelled",
      payload: { order_hash: orderHash, market_id: row.market_id, cancelled_by: agent.agentId },
    });
  });
  removeBookOrder({ market_id: row.market_id, order_hash: orderHash });
  const cancelledOrder = {
    ...normalizeOpenOrder(row),
    status: "cancelled",
    remaining_shares: 0,
  };
  broadcastAgent(
    {
      type: "order.cancelled",
      payload: {
        order_hash: orderHash,
        market_id: row.market_id,
        status: "cancelled",
        nonce_invalidation_status: "pending_onchain",
        nonce_relay_mode: NONCE_RELAY_MODE,
        server_now_ms: Date.now(),
      },
    },
    client => client.data?.agent?.agentId === agent.agentId,
  );
  void emitAgentNotifications({
    agentId: agent.agentId,
    eventType: "order.cancelled",
    payload: {
      order_hash: orderHash,
      market_id: row.market_id,
      status: "cancelled",
      nonce_invalidation_status: "pending_onchain",
      nonce_relay_mode: NONCE_RELAY_MODE,
    },
  });
  await broadcastAgentPositionsDelta({ agentId: agent.agentId, marketId: row.market_id });
  broadcastBookDelta({
    marketId: row.market_id,
    reason: "order.cancelled",
    order: cancelledOrder,
    side: mapSideToBookKey(row.side),
    priceTenths: Number(row.price_tenths),
  });
  broadcastBook(row.market_id);
  return {
    ok: true,
    order_hash: orderHash,
    market_id: row.market_id,
    status: "cancelled",
    nonce_invalidation_status: "pending_onchain",
    nonce_relay_mode: NONCE_RELAY_MODE,
  };
};

const takeOrder = async ({ agent, payload }) => {
  if (!ledgerPool) return agentError("LEDGER_UNAVAILABLE");
  const activeMarketId = getCurrentMarketId(state.currentRound);
  if (!state.currentRound || state.currentRound.status !== "live" || !activeMarketId) {
    return agentError("MARKET_NOT_LIVE_FOR_FILLS");
  }
  const orderHash = payload?.order_hash ?? null;
  if (!orderHash) return agentError("ORDER_HASH_REQUIRED");
  const fillShares = Number(payload?.size_shares);
  if (!Number.isFinite(fillShares) || fillShares <= 0) {
    return agentError("INVALID_SIZE_SHARES");
  }
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_orders
      where order_hash = $1
      limit 1
    `,
    [orderHash],
  );
  const row = rows[0];
  if (!row) return agentError("ORDER_NOT_FOUND");
  if (row.market_id !== activeMarketId) {
    return agentError("ORDER_NOT_ACTIVE_MARKET");
  }
  if (!DEFAULT_OPEN_ORDER_STATUSES.has(row.status)) {
    return agentError("ORDER_NOT_OPEN", `Order is not fillable: ${row.status}.`);
  }
  if (row.agent_id && row.agent_id === agent.agentId) {
    return agentError("SELF_TRADE_BLOCKED");
  }
  const remainingShares = Number(row.remaining_shares);
  if (!Number.isFinite(remainingShares) || remainingShares <= 0) {
    return agentError("ORDER_NO_REMAINING_SHARES");
  }
  const normalizedFillShares = Number(Math.min(fillShares, remainingShares).toFixed(8));
  const nextRemainingShares = Number((remainingShares - normalizedFillShares).toFixed(8));
  const nextStatus = nextRemainingShares > 0 ? "partially_filled" : "filled";
  const takerAddress =
    typeof payload?.taker_address === "string" && payload.taker_address ? payload.taker_address : `agent:${agent.agentId}`;
  let txHash = null;
  if (FILL_SETTLEMENT_MODE === "abstract_testnet") {
    if (!FILL_MANAGER_ADDRESS && !CLEARINGHOUSE_ADDRESS) {
      return agentError("FILL_ONCHAIN_CONFIG_MISSING");
    }
    if (row.order_kind !== "signed" || !row.signature) {
      return agentError("FILL_REQUIRES_SIGNED_ORDER");
    }
    const clients = createAbstractRelayClients();
    const contractOrder = {
      marketId: row.market_id,
      maker: getAddress(row.maker_address),
      side: row.side,
      priceTenths: Number(row.price_tenths),
      sizeMicroshares: sharesToMicroshares(Number(row.size_shares)),
      nonce: BigInt(Number(row.nonce)),
      expiryMs: BigInt(Number(row.expiry_ms)),
    };
    const fillMicroshares = sharesToMicroshares(normalizedFillShares);
    try {
      const contractAddress = CLEARINGHOUSE_ADDRESS ?? FILL_MANAGER_ADDRESS;
      const contractAbi = CLEARINGHOUSE_ADDRESS ? CLEARINGHOUSE_ABI : FILL_MANAGER_ABI;
      await clients.publicClient.simulateContract({
        address: contractAddress,
        abi: contractAbi,
        functionName: "fillOrder",
        args: [contractOrder, row.signature, fillMicroshares],
        account: clients.account.address,
      });
      txHash = await clients.walletClient.writeContract({
        address: contractAddress,
        abi: contractAbi,
        functionName: "fillOrder",
        args: [contractOrder, row.signature, fillMicroshares],
        account: clients.account,
        chain: abstractTestnet,
      });
      const receipt = await clients.publicClient.waitForTransactionReceipt({ hash: txHash });
      if (receipt.status !== "success") {
        return agentError("FILL_ONCHAIN_FAILED", `Onchain fill failed with status ${receipt.status}.`);
      }
    } catch (error) {
      recordOnchainFailure({ kind: "fill", error });
      return agentError(
        "FILL_ONCHAIN_REJECTED",
        `Onchain fill rejected: ${error?.shortMessage ?? error?.message ?? "unknown error"}`,
      );
    }
  }
  const fill = {
    market_id: row.market_id,
    order_hash: row.order_hash,
    maker_address: row.maker_address,
    maker_agent_id: row.agent_id ?? null,
    taker_address: takerAddress,
    taker_agent_id: agent.agentId,
    side: row.side,
    price_tenths: Number(row.price_tenths),
    fill_shares: normalizedFillShares,
    tx_hash: txHash,
  };
  await enqueueLedgerWrite(async () => {
    await ledgerPool.query(
      `
        update prediction_market_orders
        set remaining_shares = $2,
            status = $3,
            updated_at = now()
        where order_hash = $1
      `,
      [orderHash, nextRemainingShares, nextStatus],
    );
    await persistFill(fill);
    await appendOrderEvent({
      orderHash,
      marketId: row.market_id,
      eventType: "order.filled",
      payload: {
        order_hash: orderHash,
        market_id: row.market_id,
        maker_agent_id: row.agent_id ?? null,
        taker_agent_id: agent.agentId,
        side: row.side,
        price_tenths: Number(row.price_tenths),
        fill_shares: normalizedFillShares,
        remaining_shares: nextRemainingShares,
        status: nextStatus,
      },
    });
  });
  if (nextRemainingShares > 0) {
    upsertBookOrder({
      ...normalizeOpenOrder(row),
      remaining_shares: nextRemainingShares,
      status: nextStatus,
    });
  } else {
    removeBookOrder({ market_id: row.market_id, order_hash: orderHash });
  }
  const payloadFill = {
    ...fill,
    remaining_shares: nextRemainingShares,
    status: nextStatus,
    server_now_ms: Date.now(),
  };
  broadcastAgent(
    {
      type: "fill.executed",
      payload: payloadFill,
    },
    client => client.data?.agent?.agentId === agent.agentId || client.data?.agent?.agentId === row.agent_id,
  );
  if (row.agent_id) {
    void emitAgentNotifications({
      agentId: row.agent_id,
      eventType: "fill.executed",
      payload: payloadFill,
    });
  }
  void emitAgentNotifications({
    agentId: agent.agentId,
    eventType: "fill.executed",
    payload: payloadFill,
  });
  if (row.agent_id) {
    await broadcastAgentPositionsDelta({ agentId: row.agent_id, marketId: row.market_id });
  }
  await broadcastAgentPositionsDelta({ agentId: agent.agentId, marketId: row.market_id });
  broadcastBookDelta({
    marketId: row.market_id,
    reason: "order.filled",
    order: {
      ...normalizeOpenOrder(row),
      remaining_shares: nextRemainingShares,
      status: nextStatus,
    },
    side: mapSideToBookKey(row.side),
    priceTenths: Number(row.price_tenths),
  });
  broadcastBook(row.market_id);
  return { ok: true, fill: payloadFill };
};

const updateRoundSettlementRecord = async ({ marketId, txHash, mode }) => {
  if (!ledgerPool || !marketId || !mode) return;
  const startMs = parseIntegerOrNull(String(marketId).split("-").at(-1));
  if (!Number.isFinite(startMs)) return;
  await enqueueLedgerWrite(async () => {
    await ledgerPool.query(
      `
        update prediction_market_rounds
        set resolution_tx_hash = $1,
            resolution_mode = $2,
            updated_at = now()
        where venue = $3
          and symbol = $4
          and start_ms = $5
      `,
      [txHash ?? null, mode, LEDGER_VENUE, SYMBOL, startMs],
    );
  });
};

const resolveRoundOnchain = async ({ marketId, round }) => {
  if (SETTLEMENT_MODE !== "abstract_testnet") {
    return { ok: true, txHash: null, mode: "offchain" };
  }
  if (!marketId || !round?.status || round.status !== "resolved") {
    return { ok: false, error: "Round is not resolved." };
  }
  const { publicClient, walletClient, account } = createSettlementRelayClients();
  const existing = await publicClient.readContract({
    address: SETTLEMENT_MANAGER_ADDRESS,
    abi: SETTLEMENT_MANAGER_ABI,
    functionName: "getRoundResolution",
    args: [marketId],
  });
  if (existing?.[0]) {
    return { ok: true, txHash: null, mode: "abstract_testnet", alreadyResolved: true };
  }
  const resultCode = resultToResultCode(round.result);
  const resolvedAtMs = BigInt(round.resolvedAtMs ?? Date.now());
  const { request } = await publicClient.simulateContract({
    address: SETTLEMENT_MANAGER_ADDRESS,
    abi: SETTLEMENT_MANAGER_ABI,
    functionName: "resolveRound",
    args: [marketId, resultCode, resolvedAtMs],
    account,
  });
  const txHash = await walletClient.writeContract(request);
  const receipt = await publicClient.waitForTransactionReceipt({ hash: txHash });
  if (receipt.status !== "success") {
    recordOnchainFailure({ kind: "settlement_resolve", error: `Settlement resolve tx failed with status ${receipt.status}.` });
    return { ok: false, error: `Settlement resolve tx failed with status ${receipt.status}.`, txHash };
  }
  return { ok: true, txHash, mode: "abstract_testnet" };
};

const submitClaimOnchain = async ({ marketId, claimantAddress }) => {
  if (SETTLEMENT_MODE !== "abstract_testnet") {
    return { ok: true, txHash: null, mode: "offchain" };
  }
  if (!marketId || !claimantAddress) {
    return agentError("CLAIM_CLAIMANT_ADDRESS_REQUIRED", "market_id and claimant_address are required for onchain claims.");
  }
  const claimant = getAddress(claimantAddress);
  const { publicClient, walletClient, account } = createSettlementRelayClients();
  const claimedMicrousd = await publicClient.readContract({
    address: SETTLEMENT_MANAGER_ADDRESS,
    abi: SETTLEMENT_MANAGER_ABI,
    functionName: "claimedMicrousd",
    args: [marketIdToContractKey(marketId), claimant],
  });
  if (BigInt(claimedMicrousd) > 0n) {
    return agentError("CLAIM_ALREADY_ONCHAIN");
  }
  const previewClaimMicrousd = await publicClient.readContract({
    address: SETTLEMENT_MANAGER_ADDRESS,
    abi: SETTLEMENT_MANAGER_ABI,
    functionName: "previewClaimMicrousd",
    args: [marketId, claimant],
  });
  if (BigInt(previewClaimMicrousd) === 0n) {
    return agentError("CLAIM_NOTHING_CLAIMABLE", "No claimable balance remains onchain.");
  }
  const { request } = await publicClient.simulateContract({
    address: SETTLEMENT_MANAGER_ADDRESS,
    abi: SETTLEMENT_MANAGER_ABI,
    functionName: "claim",
    args: [marketId, claimant],
    account,
  });
  const txHash = await walletClient.writeContract(request);
  const receipt = await publicClient.waitForTransactionReceipt({ hash: txHash });
  if (receipt.status !== "success") {
    recordOnchainFailure({ kind: "claim", error: `Claim tx failed with status ${receipt.status}.` });
    return agentError("CLAIM_TX_FAILED", `Claim tx failed with status ${receipt.status}.`, { txHash });
  }
  return { ok: true, txHash, mode: "abstract_testnet", claimantAddress: claimant };
};

const submitClaim = async ({ agent, marketId, claimantAddress = null }) => {
  if (!ledgerPool) return agentError("LEDGER_UNAVAILABLE");
  if (!marketId) return agentError("CLAIM_MARKET_ID_REQUIRED");
  const positions = await listAgentPositions({ agentId: agent.agentId, marketId });
  const position = positions.find(item => item.market_id === marketId);
  if (!position) return agentError("CLAIM_NO_POSITION");
  if (position.round_status !== "resolved") {
    return agentError("CLAIM_MARKET_NOT_RESOLVED");
  }
  if (position.claimable_usd <= 0) {
    return agentError("CLAIM_NOTHING_CLAIMABLE");
  }
  let resolvedClaimantAddress = null;
  let claimMode = "offchain";
  let txHash = null;
  if (SETTLEMENT_MODE === "abstract_testnet") {
    const candidateClaimantAddress =
      claimantAddress ??
      agent.claimantAddress ??
      (isAddress(agent.agentId) ? agent.agentId : null) ??
      null;
    if (!candidateClaimantAddress || !isAddress(candidateClaimantAddress)) {
      return agentError("CLAIM_CLAIMANT_ADDRESS_REQUIRED");
    }
    const onchainClaim = await submitClaimOnchain({
      marketId,
      claimantAddress: candidateClaimantAddress,
    });
    if (!onchainClaim.ok) {
      return onchainClaim;
    }
    resolvedClaimantAddress = onchainClaim.claimantAddress ?? getAddress(candidateClaimantAddress);
    claimMode = onchainClaim.mode ?? "abstract_testnet";
    txHash = onchainClaim.txHash ?? null;
  }
  const createdAtMs = Date.now();
  await enqueueLedgerWrite(async () => {
    await ledgerPool.query(
      `
        insert into prediction_market_claims (
          market_id,
          agent_id,
          claimant_address,
          amount_usd,
          round_result,
          tx_hash,
          claim_mode,
          created_at_ms
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8)
        on conflict (market_id, agent_id) do nothing
      `,
      [
        marketId,
        agent.agentId,
        resolvedClaimantAddress,
        position.claimable_usd,
        position.round_result ?? "flat",
        txHash,
        claimMode,
        createdAtMs,
      ],
    );
  });
  const claims = await listAgentClaims({ agentId: agent.agentId, marketId, limit: 1 });
  const claim = claims[0];
  if (!claim) {
    return agentError("CLAIM_ALREADY_SUBMITTED");
  }
  await broadcastAgentPositionsDelta({ agentId: agent.agentId, marketId });
  broadcastAgent(
    {
      type: "claim.accepted",
      payload: {
        claim,
        server_now_ms: Date.now(),
      },
    },
    client => client.data?.agent?.agentId === agent.agentId,
  );
  void emitAgentNotifications({
    agentId: agent.agentId,
    eventType: "claim.accepted",
    payload: {
      claim,
      market_id: marketId,
    },
  });
  return { ok: true, claim };
};

const listAgentOrders = async ({ agentId, marketId = null, limit = ORDER_PAGE_SIZE_DEFAULT }) => {
  if (!ledgerPool) return [];
  const safeLimit = Math.max(1, Math.min(ORDER_PAGE_SIZE_MAX, Math.floor(limit)));
  const params = [agentId];
  let marketClause = "";
  if (marketId) {
    params.push(marketId);
    marketClause = `and market_id = $${params.length}`;
  }
  params.push(safeLimit);
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_orders
      where agent_id = $1
        ${marketClause}
      order by accepted_at_ms desc
      limit $${params.length}
    `,
    params,
  );
  return rows.map(normalizeOpenOrder);
};

const getAgentOrderByHash = async ({ agentId, orderHash }) => {
  if (!ledgerPool || !orderHash) return null;
  const row = await findOrderByHash(orderHash);
  if (!row) return null;
  if (row.agent_id && row.agent_id !== agentId) return null;
  return normalizeOpenOrder(row);
};

const listAgentPositions = async ({ agentId, marketId = null }) => {
  if (!ledgerPool) return [];
  const params = [agentId, [...DEFAULT_OPEN_ORDER_STATUSES]];
  let marketClause = "";
  if (marketId) {
    params.push(marketId);
    marketClause = `and market_id = $${params.length}`;
  }
  const venueParam = params.length + 1;
  const symbolParam = params.length + 2;
  const { rows } = await ledgerPool.query(
    `
      with open_orders as (
        select
          market_id,
          sum(case when side = 'buy_yes' and status = any($2::text[]) then remaining_shares else 0 end) as open_buy_yes_shares,
          sum(case when side = 'sell_yes' and status = any($2::text[]) then remaining_shares else 0 end) as open_sell_yes_shares,
          sum(
            case
              when side = 'buy_yes' and status = any($2::text[]) then remaining_shares * (price_tenths::numeric / 1000.0)
              when side = 'sell_yes' and status = any($2::text[]) then remaining_shares * ((1000 - price_tenths)::numeric / 1000.0)
              else 0
            end
          ) as reserved_collateral_usd,
          count(*) filter (where status = any($2::text[])) as open_order_count
        from prediction_market_orders
        where agent_id = $1
          ${marketClause}
        group by market_id
      ),
      fill_rollup as (
        select
          market_id,
          sum(
            case
              when maker_agent_id = $1 and side = 'buy_yes' then fill_shares
              when maker_agent_id = $1 and side = 'sell_yes' then 0
              when taker_agent_id = $1 and side = 'sell_yes' then fill_shares
              else 0
            end
          ) as bought_yes_shares,
          sum(
            case
              when maker_agent_id = $1 and side = 'sell_yes' then fill_shares
              when maker_agent_id = $1 and side = 'buy_yes' then 0
              when taker_agent_id = $1 and side = 'buy_yes' then fill_shares
              else 0
            end
          ) as sold_yes_shares,
          sum(
            case
              when maker_agent_id = $1 and side = 'buy_yes' then fill_shares
              when maker_agent_id = $1 and side = 'sell_yes' then -fill_shares
              when taker_agent_id = $1 and side = 'sell_yes' then fill_shares
              when taker_agent_id = $1 and side = 'buy_yes' then -fill_shares
              else 0
            end
          ) as net_yes_shares,
          count(*) filter (where maker_agent_id = $1 or taker_agent_id = $1) as fill_count
        from prediction_market_fills
        where (maker_agent_id = $1 or taker_agent_id = $1)
          ${marketClause}
        group by market_id
      ),
      claim_rollup as (
        select
          market_id,
          sum(amount_usd) as claimed_usd
        from prediction_market_claims
        where agent_id = $1
          ${marketClause}
        group by market_id
      ),
      round_rollup as (
        select
          start_ms,
          status as round_status,
          result as round_result
        from prediction_market_rounds
        where venue = $${venueParam}
          and symbol = $${symbolParam}
      )
      select
        coalesce(o.market_id, f.market_id) as market_id,
        coalesce(o.open_buy_yes_shares, 0) as open_buy_yes_shares,
        coalesce(o.open_sell_yes_shares, 0) as open_sell_yes_shares,
        coalesce(o.reserved_collateral_usd, 0) as reserved_collateral_usd,
        coalesce(o.open_order_count, 0) as open_order_count,
        coalesce(f.bought_yes_shares, 0) as bought_yes_shares,
        coalesce(f.sold_yes_shares, 0) as sold_yes_shares,
        coalesce(f.net_yes_shares, 0) as net_yes_shares,
        coalesce(f.fill_count, 0) as fill_count,
        coalesce(c.claimed_usd, 0) as claimed_usd,
        r.round_status,
        r.round_result
      from open_orders o
      full outer join fill_rollup f on f.market_id = o.market_id
      left join claim_rollup c on c.market_id = coalesce(o.market_id, f.market_id)
      left join round_rollup r
        on r.start_ms = nullif(split_part(coalesce(o.market_id, f.market_id), '-', 3), '')::bigint
      order by coalesce(o.market_id, f.market_id) desc
    `,
    [...params, LEDGER_VENUE, SYMBOL],
  );
  const chainAddress = resolveAgentChainAddress({ agentId, claimantAddress: null });
  const authAgent = await getAgentAuthRecord({ agentId });
  const resolvedChainAddress = resolveAgentChainAddress(authAgent ?? { agentId, claimantAddress: chainAddress });
  const vaultState = resolvedChainAddress && isVaultBalanceModeEnabled() ? await readVaultAccountState(resolvedChainAddress) : null;
  const items = [];
  for (const row of rows) {
    let netYesShares = Number(row.net_yes_shares ?? 0);
    if (resolvedChainAddress && isVaultBalanceModeEnabled() && row.market_id) {
      const onchainNetYes = await readClearinghouseNetYesShares({
        marketId: row.market_id,
        traderAddress: resolvedChainAddress,
      });
      if (Number.isFinite(onchainNetYes)) {
        netYesShares = Number(onchainNetYes);
      }
    }
    const grossClaimableUsd =
      row.round_status === "resolved"
        ? normalizeUsd(
            row.round_result === "up"
              ? Math.max(0, netYesShares)
              : row.round_result === "down"
                ? Math.max(0, -netYesShares)
                : 0,
          )
        : 0;
    items.push({
      market_id: row.market_id,
      round_status: row.round_status ?? null,
      round_result: row.round_result ?? null,
      open_buy_yes_shares: Number(row.open_buy_yes_shares ?? 0),
      open_sell_yes_shares: Number(row.open_sell_yes_shares ?? 0),
      reserved_collateral_usd: normalizeUsd(Number(row.reserved_collateral_usd ?? 0)),
      open_order_count: Number(row.open_order_count ?? 0),
      bought_yes_shares: Number(row.bought_yes_shares ?? 0),
      sold_yes_shares: Number(row.sold_yes_shares ?? 0),
      net_yes_shares: netYesShares,
      fill_count: Number(row.fill_count ?? 0),
      claimed_usd: normalizeUsd(Number(row.claimed_usd ?? 0)),
      gross_claimable_usd: grossClaimableUsd,
      claimable_usd:
        row.round_status === "resolved"
          ? normalizeUsd(Math.max(0, grossClaimableUsd - Number(row.claimed_usd ?? 0)))
          : 0,
      balance_source: vaultState ? "vault" : "ledger",
      chain_address: resolvedChainAddress,
      onchain_available_collateral_usd: vaultState ? microusdToUsd(vaultState.availableMicrousd) : null,
      onchain_reserved_collateral_usd: vaultState ? microusdToUsd(vaultState.reservedMicrousd) : null,
      onchain_claimable_collateral_usd: vaultState ? microusdToUsd(vaultState.claimableMicrousd) : null,
      effective_available_collateral_usd: vaultState
        ? normalizeUsd(Math.max(0, microusdToUsd(vaultState.availableMicrousd) - Number(row.reserved_collateral_usd ?? 0)))
        : null,
    });
  }
  if (!items.length && vaultState && resolvedChainAddress) {
    items.push({
      market_id: marketId ?? getCurrentMarketId(state.currentRound),
      round_status: marketId === getCurrentMarketId(state.currentRound) ? state.currentRound?.status ?? null : null,
      round_result: marketId === getCurrentMarketId(state.currentRound) ? state.currentRound?.result ?? null : null,
      open_buy_yes_shares: 0,
      open_sell_yes_shares: 0,
      reserved_collateral_usd: 0,
      open_order_count: 0,
      bought_yes_shares: 0,
      sold_yes_shares: 0,
      net_yes_shares: 0,
      fill_count: 0,
      claimed_usd: 0,
      gross_claimable_usd: 0,
      claimable_usd: 0,
      balance_source: "vault",
      chain_address: resolvedChainAddress,
      onchain_available_collateral_usd: microusdToUsd(vaultState.availableMicrousd),
      onchain_reserved_collateral_usd: microusdToUsd(vaultState.reservedMicrousd),
      onchain_claimable_collateral_usd: microusdToUsd(vaultState.claimableMicrousd),
      effective_available_collateral_usd: microusdToUsd(vaultState.availableMicrousd),
    });
  }
  return items;
};

const listAgentClaims = async ({ agentId, marketId = null, limit = ORDER_PAGE_SIZE_DEFAULT }) => {
  if (!ledgerPool) return [];
  const safeLimit = Math.max(1, Math.min(ORDER_PAGE_SIZE_MAX, Math.floor(limit)));
  const params = [agentId];
  let marketClause = "";
  if (marketId) {
    params.push(marketId);
    marketClause = `and market_id = $${params.length}`;
  }
  params.push(safeLimit);
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_claims
      where agent_id = $1
        ${marketClause}
      order by created_at_ms desc
      limit $${params.length}
    `,
    params,
  );
  return rows.map(row => ({
    id: Number(row.id),
    market_id: row.market_id,
    agent_id: row.agent_id,
    claimant_address: row.claimant_address ?? null,
    amount_usd: normalizeUsd(Number(row.amount_usd ?? 0)),
    round_result: row.round_result,
    tx_hash: row.tx_hash ?? null,
    claim_mode: row.claim_mode ?? "offchain",
    created_at_ms: Number(row.created_at_ms),
  }));
};

const listAgentFills = async ({ agentId, marketId = null, limit = FILL_PAGE_SIZE_DEFAULT }) => {
  if (!ledgerPool) return [];
  const safeLimit = Math.max(1, Math.min(FILL_PAGE_SIZE_MAX, Math.floor(limit)));
  const params = [agentId];
  let marketClause = "";
  if (marketId) {
    params.push(marketId);
    marketClause = `and market_id = $${params.length}`;
  }
  params.push(safeLimit);
  const { rows } = await ledgerPool.query(
    `
      select
        id,
        market_id,
        order_hash,
        maker_address,
        maker_agent_id,
        taker_address,
        taker_agent_id,
        side,
        price_tenths,
        fill_shares,
        tx_hash,
        extract(epoch from created_at) * 1000 as created_at_ms
      from prediction_market_fills
      where (maker_agent_id = $1 or taker_agent_id = $1)
        ${marketClause}
      order by id desc
      limit $${params.length}
    `,
    params,
  );
  return rows.map(row => ({
    id: Number(row.id),
    market_id: row.market_id,
    order_hash: row.order_hash,
    maker_address: row.maker_address,
    maker_agent_id: row.maker_agent_id,
    taker_address: row.taker_address,
    taker_agent_id: row.taker_agent_id,
    role: row.maker_agent_id === agentId ? "maker" : "taker",
    side: row.side,
    price_tenths: Number(row.price_tenths),
    fill_shares: Number(row.fill_shares),
    tx_hash: row.tx_hash,
    created_at_ms: Number(row.created_at_ms),
  }));
};

const listAgentOrderEvents = async ({ agentId, marketId = null, limit = ORDER_PAGE_SIZE_DEFAULT }) => {
  if (!ledgerPool) return [];
  const safeLimit = Math.max(1, Math.min(ORDER_PAGE_SIZE_MAX, Math.floor(limit)));
  const params = [agentId];
  let marketClause = "";
  if (marketId) {
    params.push(marketId);
    marketClause = `and e.market_id = $${params.length}`;
  }
  params.push(safeLimit);
  const { rows } = await ledgerPool.query(
    `
      select
        e.id,
        e.order_hash,
        e.market_id,
        e.event_type,
        e.payload,
        extract(epoch from e.created_at) * 1000 as created_at_ms
      from prediction_market_order_events e
      join prediction_market_orders o on o.order_hash = e.order_hash
      where o.agent_id = $1
        ${marketClause}
      order by e.id desc
      limit $${params.length}
    `,
    params,
  );
  return rows.map(row => ({
    id: Number(row.id),
    order_hash: row.order_hash,
    market_id: row.market_id,
    event_type: row.event_type,
    payload: row.payload,
    created_at_ms: Number(row.created_at_ms),
  }));
};

const buildAgentCapabilities = agent => ({
  order_place: agent?.scopes?.has?.("*") || agent?.scopes?.has?.("admin") || agent?.scopes?.has?.("orders:write"),
  order_cancel: agent?.scopes?.has?.("*") || agent?.scopes?.has?.("admin") || agent?.scopes?.has?.("orders:cancel"),
  order_take: agent?.scopes?.has?.("*") || agent?.scopes?.has?.("admin") || agent?.scopes?.has?.("fills:take"),
  fills_onchain: FILL_SETTLEMENT_MODE === "abstract_testnet",
  claims_onchain: SETTLEMENT_MODE === "abstract_testnet",
  nonce_relay_mode: NONCE_RELAY_MODE,
  settlement_mode: SETTLEMENT_MODE,
});

const buildAgentMePayload = async agent => {
  const collateralCapacity = await getAgentCollateralCapacity(agent);
  return {
    protocol_version: AGENT_PROTOCOL_VERSION,
    server_now_ms: Date.now(),
    agent_id: agent.agentId,
    key_id: agent.keyId ?? null,
    key_prefix: agent.keyPrefix ?? null,
    source: agent.source ?? null,
    claimant_address: agent.claimantAddress ?? null,
    scopes: scopeSetToArray(agent.scopes ?? new Set()),
    rate_limit_per_minute: agent.rateLimitPerMinute ?? null,
    collateral: {
      source: collateralCapacity.source,
      total_usd: collateralCapacity.totalUsd,
      reserved_usd: collateralCapacity.reservedUsd,
      effective_available_usd: collateralCapacity.effectiveAvailableUsd,
      chain_address: collateralCapacity.chainAddress ?? null,
    },
    capabilities: buildAgentCapabilities(agent),
  };
};

const buildAgentAccountSummary = async agent => {
  const [positions, orders, claims, fills, collateralCapacity] = await Promise.all([
    listAgentPositions({ agentId: agent.agentId }),
    listAgentOrders({ agentId: agent.agentId, limit: ORDER_PAGE_SIZE_MAX }),
    listAgentClaims({ agentId: agent.agentId, limit: ORDER_PAGE_SIZE_MAX }),
    listAgentFills({ agentId: agent.agentId, limit: FILL_PAGE_SIZE_MAX }),
    getAgentCollateralCapacity(agent),
  ]);
  return {
    protocol_version: AGENT_PROTOCOL_VERSION,
    server_now_ms: Date.now(),
    agent_id: agent.agentId,
    current_market_id: getCurrentMarketId(state.currentRound),
    open_order_count: orders.filter(order => DEFAULT_OPEN_ORDER_STATUSES.has(order.status)).length,
    open_orders_reserved_collateral_usd: normalizeUsd(
      positions.reduce((sum, item) => sum + Number(item.reserved_collateral_usd ?? 0), 0),
    ),
    net_yes_shares: Number(
      positions.reduce((sum, item) => sum + Number(item.net_yes_shares ?? 0), 0).toFixed(8),
    ),
    gross_claimable_usd: normalizeUsd(
      positions.reduce((sum, item) => sum + Number(item.gross_claimable_usd ?? 0), 0),
    ),
    claimable_usd: normalizeUsd(
      positions.reduce((sum, item) => sum + Number(item.claimable_usd ?? 0), 0),
    ),
    claimed_usd: normalizeUsd(
      claims.reduce((sum, item) => sum + Number(item.amount_usd ?? 0), 0),
    ),
    fill_count: fills.length,
    collateral: {
      source: collateralCapacity.source,
      total_usd: collateralCapacity.totalUsd,
      reserved_usd: collateralCapacity.reservedUsd,
      effective_available_usd: collateralCapacity.effectiveAvailableUsd,
      chain_address: collateralCapacity.chainAddress ?? null,
    },
  };
};

const listMarketsDirectory = async () => {
  const currentMarketId = getCurrentMarketId(state.currentRound);
  const resolved = await listResolvedRoundsPage({ limit: RESOLUTION_PAGE_SIZE_DEFAULT });
  return {
    protocol_version: AGENT_PROTOCOL_VERSION,
    symbol: SYMBOL,
    current_market_id: currentMarketId,
    current_round: state.currentRound,
    previous_round: state.previousRound,
    resolved_markets: resolved.items.map(round => ({
      market_id: round?.startMs ? `${SYMBOL}-5m-${round.startMs}` : null,
      ...round,
    })),
    next_cursor: resolved.nextCursor,
  };
};

const normalizeNotificationEvents = events => {
  const list = Array.isArray(events) ? events.filter(Boolean).map(String) : [];
  const filtered = list.filter(value => AGENT_NOTIFICATION_EVENT_TYPES.includes(value));
  return [...new Set(filtered.length ? filtered : AGENT_NOTIFICATION_EVENT_TYPES)].sort();
};

const serializeAgentNotificationRow = row => ({
  id: String(row.id),
  agent_id: row.agent_id,
  provider: row.provider,
  label: row.label ?? null,
  status: row.status,
  webhook_url: row.provider === "discord" ? row.webhook_url ?? null : null,
  telegram_chat_id: row.provider === "telegram" ? row.telegram_chat_id ?? null : null,
  events: normalizeNotificationEvents(row.events),
  last_delivery_at_ms: row.last_delivery_at ? new Date(row.last_delivery_at).getTime() : null,
  last_error: row.last_error ?? null,
  created_at_ms: row.created_at ? new Date(row.created_at).getTime() : null,
});

const listAgentNotifications = async ({ agentId }) => {
  if (!ledgerPool) return [];
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_agent_notifications
      where agent_id = $1
      order by id desc
    `,
    [agentId],
  );
  return rows.map(serializeAgentNotificationRow);
};

const validateNotificationConfig = ({ provider, webhookUrl = null, telegramBotToken = null, telegramChatId = null, events }) => {
  const normalizedProvider = typeof provider === "string" ? provider.trim() : "";
  if (!["discord", "telegram"].includes(normalizedProvider)) {
    return { ok: false, error: "provider must be discord or telegram." };
  }
  if (normalizedProvider === "discord" && !(typeof webhookUrl === "string" && /^https?:\/\//.test(webhookUrl))) {
    return { ok: false, error: "discord notifications require a valid webhook_url." };
  }
  if (normalizedProvider === "telegram" && !(typeof telegramBotToken === "string" && telegramBotToken.length > 0 && typeof telegramChatId === "string" && telegramChatId.length > 0)) {
    return { ok: false, error: "telegram notifications require bot_token and chat_id." };
  }
  return {
    ok: true,
    provider: normalizedProvider,
    events: normalizeNotificationEvents(events),
  };
};

const createAgentNotification = async ({ agentId, provider, label = null, webhookUrl = null, telegramBotToken = null, telegramChatId = null, events }) => {
  if (!ledgerPool) return agentError("LEDGER_UNAVAILABLE");
  const validation = validateNotificationConfig({ provider, webhookUrl, telegramBotToken, telegramChatId, events });
  if (!validation.ok) return { ok: false, error: validation.error, status: 400 };
  const { rows } = await ledgerPool.query(
    `
      insert into prediction_market_agent_notifications (
        agent_id,
        provider,
        label,
        webhook_url,
        telegram_bot_token,
        telegram_chat_id,
        events,
        status,
        updated_at
      )
      values ($1,$2,$3,$4,$5,$6,$7::jsonb,'active', now())
      returning *
    `,
    [
      agentId,
      validation.provider,
      label ?? null,
      validation.provider === "discord" ? webhookUrl : null,
      validation.provider === "telegram" ? telegramBotToken : null,
      validation.provider === "telegram" ? telegramChatId : null,
      JSON.stringify(validation.events),
    ],
  );
  return { ok: true, item: serializeAgentNotificationRow(rows[0]) };
};

const updateAgentNotification = async ({ agentId, id, label, status, events }) => {
  if (!ledgerPool) return agentError("LEDGER_UNAVAILABLE");
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_agent_notifications
      where id = $1
        and agent_id = $2
      limit 1
    `,
    [id, agentId],
  );
  const current = rows[0] ?? null;
  if (!current) return { ok: false, error: "Notification destination not found.", status: 404 };
  const nextEvents = events === undefined ? current.events : normalizeNotificationEvents(events);
  const nextStatus = status === undefined ? current.status : String(status);
  if (!["active", "disabled"].includes(nextStatus)) {
    return { ok: false, error: "status must be active or disabled.", status: 400 };
  }
  const { rows: updatedRows } = await ledgerPool.query(
    `
      update prediction_market_agent_notifications
      set label = $3,
          status = $4,
          events = $5::jsonb,
          updated_at = now()
      where id = $1
        and agent_id = $2
      returning *
    `,
    [id, agentId, label === undefined ? current.label : label, nextStatus, JSON.stringify(nextEvents)],
  );
  return { ok: true, item: serializeAgentNotificationRow(updatedRows[0]) };
};

const deleteAgentNotification = async ({ agentId, id }) => {
  if (!ledgerPool) return agentError("LEDGER_UNAVAILABLE");
  const { rows } = await ledgerPool.query(
    `
      delete from prediction_market_agent_notifications
      where id = $1
        and agent_id = $2
      returning *
    `,
    [id, agentId],
  );
  if (!rows[0]) return { ok: false, error: "Notification destination not found.", status: 404 };
  return { ok: true, deleted: true, id: String(rows[0].id) };
};

const renderNotificationText = ({ eventType, agentId, payload }) => {
  const lines = [`prediction-markets ${eventType}`, `agent: ${agentId}`];
  if (payload?.market_id) lines.push(`market: ${payload.market_id}`);
  if (payload?.order?.order_hash) lines.push(`order: ${payload.order.order_hash}`);
  if (payload?.order_hash) lines.push(`order: ${payload.order_hash}`);
  if (payload?.fill_shares) lines.push(`fill_shares: ${payload.fill_shares}`);
  if (payload?.price_tenths) lines.push(`price_tenths: ${payload.price_tenths}`);
  if (payload?.claim?.amount_usd) lines.push(`claim_usd: ${payload.claim.amount_usd}`);
  lines.push(`server_now_ms: ${Date.now()}`);
  return lines.join("\n");
};

const deliverAgentNotification = async ({ destination, eventType, payload }) => {
  const text = renderNotificationText({
    eventType,
    agentId: destination.agent_id,
    payload,
  });
  if (destination.provider === "discord") {
    await fetch(destination.webhook_url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        content: text,
      }),
    });
    return;
  }
  if (destination.provider === "telegram") {
    const endpoint = `https://api.telegram.org/bot${destination.telegram_bot_token}/sendMessage`;
    await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: destination.telegram_chat_id,
        text,
      }),
    });
  }
};

const emitAgentNotifications = async ({ agentId, eventType, payload }) => {
  if (!ledgerPool || !agentId || !AGENT_NOTIFICATION_EVENT_TYPES.includes(eventType)) return;
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_agent_notifications
      where agent_id = $1
        and status = 'active'
      order by id desc
    `,
    [agentId],
  );
  const candidates = rows.filter(row => normalizeNotificationEvents(row.events).includes(eventType));
  await Promise.all(
    candidates.map(async row => {
      try {
        await deliverAgentNotification({
          destination: row,
          eventType,
          payload,
        });
        await ledgerPool.query(
          `
            update prediction_market_agent_notifications
            set last_delivery_at = now(),
                last_error = null,
                updated_at = now()
            where id = $1
          `,
          [row.id],
        );
      } catch (error) {
        await ledgerPool.query(
          `
            update prediction_market_agent_notifications
            set last_error = $2,
                updated_at = now()
            where id = $1
          `,
          [row.id, error instanceof Error ? error.message : String(error)],
        );
      }
    }),
  );
};

const appendSecondPoint = (timestampMs, price) => {
  if (!Number.isFinite(price)) return null;
  const secondMs = Math.floor(timestampMs / 1000) * 1000;
  const point = {
    timeMs: secondMs,
    price: Number(price),
  };
  const previous = state.chartSeries[state.chartSeries.length - 1] ?? null;
  if (previous && previous.timeMs === secondMs) {
    state.chartSeries[state.chartSeries.length - 1] = point;
  } else {
    state.chartSeries.push(point);
  }
  state.chartSeries = state.chartSeries.filter(entry => entry.timeMs >= secondMs - CHART_WINDOW_MS);
  return point;
};

const processNonceRelayTick = async () => {
  if (!ledgerPool || NONCE_RELAY_MODE === "disabled") return;
  const { rows } = await ledgerPool.query(
    `
      select *
      from prediction_market_nonce_invalidations
      where status = 'pending_onchain'
      order by id asc
      limit 20
    `,
  );
  for (const row of rows) {
    const createdAtMs = Number(new Date(row.created_at).getTime());
    try {
      if (NONCE_RELAY_MODE === "local_stub") {
        const submittedAtMs = Date.now();
        const txHash = `relay_${row.id.toString(36)}_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
        const claimResult = await ledgerPool.query(
          `
            update prediction_market_nonce_invalidations
            set status = 'submitted_local_stub',
                relay_mode = $2,
                tx_hash = $3,
                submitted_at_ms = $4,
                error_message = null
            where id = $1
              and status = 'pending_onchain'
          `,
          [row.id, NONCE_RELAY_MODE, txHash, submittedAtMs],
        );
        if (claimResult.rowCount === 0) continue;
        broadcastNonceInvalidationUpdate({
          id: Number(row.id),
          market_id: row.market_id,
          maker_address: row.maker_address,
          nonce: Number(row.nonce),
          agent_id: row.agent_id,
          status: "submitted_local_stub",
          relay_mode: NONCE_RELAY_MODE,
          tx_hash: txHash,
          error_message: null,
          submitted_at_ms: submittedAtMs,
          confirmed_at_ms: null,
          created_at_ms: createdAtMs,
        });
        const confirmedAtMs = submittedAtMs + 1;
        const confirmResult = await ledgerPool.query(
          `
            update prediction_market_nonce_invalidations
            set status = 'confirmed_local_stub',
                confirmed_at_ms = $2,
                error_message = null
            where id = $1
              and status = 'submitted_local_stub'
          `,
          [row.id, confirmedAtMs],
        );
        if (confirmResult.rowCount === 0) continue;
        broadcastNonceInvalidationUpdate({
          id: Number(row.id),
          market_id: row.market_id,
          maker_address: row.maker_address,
          nonce: Number(row.nonce),
          agent_id: row.agent_id,
          status: "confirmed_local_stub",
          relay_mode: NONCE_RELAY_MODE,
          tx_hash: txHash,
          error_message: null,
          submitted_at_ms: submittedAtMs,
          confirmed_at_ms: confirmedAtMs,
          created_at_ms: createdAtMs,
        });
        continue;
      }

      if (NONCE_RELAY_MODE === "abstract_testnet") {
        const clients = createNonceRelayClients();
        const submittedAtMs = Date.now();
        const claimResult = await ledgerPool.query(
          `
            update prediction_market_nonce_invalidations
            set status = 'submitting_onchain',
                relay_mode = $2,
                submitted_at_ms = $3,
                error_message = null
            where id = $1
              and status = 'pending_onchain'
          `,
          [row.id, NONCE_RELAY_MODE, submittedAtMs],
        );
        if (claimResult.rowCount === 0) continue;
        const hash = await clients.walletClient.writeContract({
          address: NONCE_MANAGER_ADDRESS,
          abi: NONCE_MANAGER_ABI,
          functionName: "invalidateNonce",
          args: [marketIdToContractKey(row.market_id), row.maker_address, BigInt(row.nonce)],
          account: clients.account,
          chain: abstractTestnet,
        });
        const submitResult = await ledgerPool.query(
          `
            update prediction_market_nonce_invalidations
            set status = 'submitted_onchain',
                relay_mode = $2,
                tx_hash = $3,
                submitted_at_ms = $4,
                error_message = null
            where id = $1
              and status = 'submitting_onchain'
          `,
          [row.id, NONCE_RELAY_MODE, hash, submittedAtMs],
        );
        if (submitResult.rowCount === 0) continue;
        broadcastNonceInvalidationUpdate({
          id: Number(row.id),
          market_id: row.market_id,
          maker_address: row.maker_address,
          nonce: Number(row.nonce),
          agent_id: row.agent_id,
          status: "submitted_onchain",
          relay_mode: NONCE_RELAY_MODE,
          tx_hash: hash,
          error_message: null,
          submitted_at_ms: submittedAtMs,
          confirmed_at_ms: null,
          created_at_ms: createdAtMs,
        });
        const receipt = await clients.publicClient.waitForTransactionReceipt({ hash });
        const confirmedAtMs = Date.now();
        const confirmResult = await ledgerPool.query(
          `
            update prediction_market_nonce_invalidations
            set status = 'confirmed_onchain',
                confirmed_at_ms = $2,
                error_message = null
            where id = $1
              and status = 'submitted_onchain'
          `,
          [row.id, confirmedAtMs],
        );
        if (confirmResult.rowCount === 0) continue;
        broadcastNonceInvalidationUpdate({
          id: Number(row.id),
          market_id: row.market_id,
          maker_address: row.maker_address,
          nonce: Number(row.nonce),
          agent_id: row.agent_id,
          status: "confirmed_onchain",
          relay_mode: NONCE_RELAY_MODE,
          tx_hash: receipt.transactionHash,
          error_message: null,
          submitted_at_ms: submittedAtMs,
          confirmed_at_ms: confirmedAtMs,
          created_at_ms: createdAtMs,
        });
      }
    } catch (error) {
      recordOnchainFailure({ kind: "nonce_invalidation", error });
      const message = error instanceof Error ? error.message : String(error);
      const failResult = await ledgerPool.query(
        `
          update prediction_market_nonce_invalidations
          set status = 'failed_onchain',
              relay_mode = $2,
              error_message = $3
          where id = $1
            and status in ('pending_onchain', 'submitting_onchain', 'submitted_onchain')
        `,
        [row.id, NONCE_RELAY_MODE, message],
      );
      if (failResult.rowCount === 0) continue;
      broadcastNonceInvalidationUpdate({
        id: Number(row.id),
        market_id: row.market_id,
        maker_address: row.maker_address,
        nonce: Number(row.nonce),
        agent_id: row.agent_id,
        status: "failed_onchain",
        relay_mode: NONCE_RELAY_MODE,
        tx_hash: row.tx_hash ?? null,
        error_message: message,
        submitted_at_ms: parseNullableNumber(row.submitted_at_ms),
        confirmed_at_ms: parseNullableNumber(row.confirmed_at_ms),
        created_at_ms: createdAtMs,
      });
    }
  }
};

const ensureNonceRelayTimer = () => {
  if (nonceRelayTimer || NONCE_RELAY_MODE === "disabled") return;
  nonceRelayTimer = setInterval(() => {
    void processNonceRelayTick().catch(error => {
      console.error("[prediction-markets] nonce relay tick failed", error);
    });
  }, NONCE_RELAY_POLL_MS);
};

const updateRoundState = nowMs => {
  if (!state.currentRound) {
    state.currentRound = buildRound(nowMs, null, state.lastPrice);
    void persistRound(state.currentRound);
    return { type: "bootstrap", round: state.currentRound };
  }
  const currentStart = roundStartFor(nowMs);
  if (currentStart !== state.currentRound.startMs) {
    const resolved = {
      ...state.currentRound,
      currentPrice: state.lastPrice,
      closePrice: state.lastPrice,
      status: "resolved",
      result: roundResultFor({ ...state.currentRound, currentPrice: state.lastPrice }),
      resolvedAtMs: nowMs,
    };
    state.previousRound = resolved;
    state.resolvedRounds = [
      resolved,
      ...state.resolvedRounds.filter(entry => entry.startMs !== resolved.startMs),
    ].slice(0, MAX_RESOLVED_ROWS);
    state.currentRound = buildRound(nowMs, null, state.lastPrice);
    void persistRound(resolved);
    void persistRound(state.currentRound);
    void pruneResolvedRounds();
    return { type: "rollover", round: state.currentRound, resolved };
  }
  state.currentRound = {
    ...state.currentRound,
    currentPrice: state.lastPrice,
    status: Number.isFinite(state.currentRound.targetPrice)
      ? roundStatusFor(nowMs, state.currentRound)
      : "waiting_for_open",
    result: roundResultFor({ ...state.currentRound, currentPrice: state.lastPrice }),
  };
  void persistRound(state.currentRound);
  return { type: "update", round: state.currentRound };
};

const tickSecond = () => {
  const nowMs = Date.now();
  const point = appendSecondPoint(nowMs, state.lastPrice);
  const roundUpdate = updateRoundState(nowMs);
  if (point) {
    broadcastPublic({
      type: "pm.second",
      payload: {
        serverNowMs: nowMs,
        point,
        lastPrice: state.lastPrice,
      },
    });
    broadcastAgent({
      type: "market.state",
      payload: {
        market_id: getCurrentMarketId(state.currentRound),
        server_now_ms: nowMs,
        current_round: state.currentRound,
        last_price: state.lastPrice,
        quote: state.quote,
      },
    });
  }
  if (roundUpdate?.type === "rollover" && roundUpdate.resolved) {
    const resolvedMarketId = `${SYMBOL}-5m-${roundUpdate.resolved.startMs}`;
    void (async () => {
      const settlementResult = await resolveRoundOnchain({
        marketId: resolvedMarketId,
        round: roundUpdate.resolved,
      }).catch(error => ({ ok: false, error: error instanceof Error ? error.message : String(error) }));
      if (!settlementResult?.ok) {
        console.error("[prediction-markets] onchain settlement resolve failed", {
          marketId: resolvedMarketId,
          error: settlementResult?.error ?? "unknown",
        });
        return;
      }
      if (settlementResult.txHash || settlementResult.mode) {
        await updateRoundSettlementRecord({
          marketId: resolvedMarketId,
          txHash: settlementResult.txHash ?? null,
          mode: settlementResult.mode ?? "offchain",
        });
      }
    })();
    void enqueueLedgerWrite(async () => {
      const expiredOrders = await expireOrdersForMarket(resolvedMarketId);
      for (const order of expiredOrders) {
        await appendOrderEvent({
          orderHash: order.order_hash,
          marketId: resolvedMarketId,
          eventType: "order.expired",
          payload: {
            order_hash: order.order_hash,
            market_id: resolvedMarketId,
            status: "expired",
          },
        });
      }
      if (expiredOrders.length) {
        clearBook(resolvedMarketId);
        for (const order of expiredOrders) {
          if (order.agent_id) {
            await broadcastAgentPositionsDelta({ agentId: order.agent_id, marketId: resolvedMarketId });
          }
        }
        broadcastBook(resolvedMarketId);
      }
    });
    const nextMarketId = getCurrentMarketId(roundUpdate.round);
    if (nextMarketId && !books.has(nextMarketId)) {
      books.set(nextMarketId, buildEmptyBook(nextMarketId));
    }
    broadcastPublic({
      type: "pm.resolve",
      payload: {
        serverNowMs: nowMs,
        round: roundUpdate.resolved,
      },
    });
    broadcastAgent({
      type: "market.resolved",
      payload: {
        market_id: `${SYMBOL}-5m-${roundUpdate.resolved.startMs}`,
        round: roundUpdate.resolved,
        server_now_ms: nowMs,
      },
    });
  }
  if (roundUpdate?.round) {
    broadcastPublic({
      type: "pm.round",
      payload: {
        serverNowMs: nowMs,
        currentRound: roundUpdate.round,
        previousRound: state.previousRound,
      },
    });
    broadcastAgent({
      type: "market.state",
      payload: {
        market_id: getCurrentMarketId(roundUpdate.round),
        server_now_ms: nowMs,
        current_round: roundUpdate.round,
        previous_round: state.previousRound,
        last_price: state.lastPrice,
        quote: state.quote,
      },
    });
  }
};

const ensureSecondTimer = () => {
  if (secondTimer) return;
  secondTimer = setInterval(() => {
    tickSecond();
  }, 1000);
};

const scheduleReconnect = () => {
  if (reconnectTimer) return;
  const delayMs = Math.min(15_000, 1_000 * 2 ** reconnectAttempt);
  reconnectAttempt += 1;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectUpstream();
  }, delayMs);
};

const connectUpstream = () => {
  if (upstreamSocket && (upstreamSocket.readyState === WebSocket.OPEN || upstreamSocket.readyState === WebSocket.CONNECTING)) {
    return;
  }
  const socket = new WebSocket(BINANCE_STREAM_URL);
  upstreamSocket = socket;

  socket.addEventListener("open", () => {
    reconnectAttempt = 0;
    state.feedConnected = true;
    state.monitoring.lastFeedEventAtMs = Date.now();
    broadcastPublic({
      type: "pm.feed_status",
      payload: {
        connected: true,
        serverNowMs: Date.now(),
      },
    });
    broadcastAgent({
      type: "feed.status",
      payload: {
        connected: true,
        server_now_ms: Date.now(),
      },
    });
  });

  socket.addEventListener("message", event => {
    let payload;
    try {
      payload = JSON.parse(typeof event.data === "string" ? event.data : String(event.data ?? ""));
    } catch {
      return;
    }
    const stream = payload?.stream ?? null;
    const data = payload?.data ?? payload;
    const eventType = data?.e ?? null;

    if (eventType === "bookTicker" || stream?.includes("bookTicker")) {
      const bidPrice = Number(data?.b);
      const bidQty = Number(data?.B);
      const askPrice = Number(data?.a);
      const askQty = Number(data?.A);
      const quoteTimeMs = Number(data?.T ?? data?.E ?? Date.now());
      if (!Number.isFinite(bidPrice) || !Number.isFinite(askPrice)) return;
      state.quote = {
        bidPrice,
        bidQty: Number.isFinite(bidQty) ? bidQty : null,
        askPrice,
        askQty: Number.isFinite(askQty) ? askQty : null,
        spread: askPrice - bidPrice,
        midPrice: (askPrice + bidPrice) / 2,
        timeMs: quoteTimeMs,
      };
      state.monitoring.lastFeedEventAtMs = quoteTimeMs;
      broadcastPublic({
        type: "pm.quote",
        payload: {
          serverNowMs: Date.now(),
          quote: state.quote,
        },
      });
      broadcastAgent({
        type: "quote.update",
        payload: {
          server_now_ms: Date.now(),
          quote: state.quote,
          market_id: getCurrentMarketId(state.currentRound),
        },
      });
      return;
    }

    const price = Number(data?.p);
    const quantity = Number(data?.q);
    const tradeTimeMs = Number(data?.T ?? data?.E ?? Date.now());
    if (!Number.isFinite(price) || !Number.isFinite(tradeTimeMs)) return;
    const tradeRoundStart = roundStartFor(tradeTimeMs);
    if (!state.currentRound || tradeRoundStart !== state.currentRound.startMs) {
      updateRoundState(tradeTimeMs);
    }
    state.lastPrice = price;
    state.lastTrade = {
      price,
      quantity: Number.isFinite(quantity) ? quantity : null,
      side: data?.m ? "sell" : "buy",
      timeMs: tradeTimeMs,
      eventTimeMs: Number(data?.E ?? tradeTimeMs),
    };
    state.monitoring.lastFeedEventAtMs = tradeTimeMs;
    if (
      state.currentRound &&
      state.currentRound.startMs === tradeRoundStart &&
      !Number.isFinite(state.currentRound.targetPrice)
    ) {
      state.currentRound = {
        ...state.currentRound,
        targetPrice: price,
        currentPrice: price,
        status: "live",
        result: "flat",
        firstTradeTimeMs: tradeTimeMs,
      };
      void persistRound(state.currentRound);
      appendSecondPoint(tradeTimeMs, price);
    }
    const roundUpdate = updateRoundState(tradeTimeMs);
    broadcastPublic({
      type: "pm.trade",
      payload: {
        serverNowMs: Date.now(),
        trade: state.lastTrade,
        currentRound: roundUpdate?.round ?? state.currentRound,
      },
    });
    broadcastAgent({
      type: "trade.update",
      payload: {
        server_now_ms: Date.now(),
        trade: state.lastTrade,
        market_id: getCurrentMarketId(state.currentRound),
      },
    });
  });

  socket.addEventListener("close", () => {
    state.feedConnected = false;
    broadcastPublic({
      type: "pm.feed_status",
      payload: {
        connected: false,
        serverNowMs: Date.now(),
      },
    });
    broadcastAgent({
      type: "feed.status",
      payload: {
        connected: false,
        server_now_ms: Date.now(),
      },
    });
    upstreamSocket = null;
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    try {
      socket.close();
    } catch {
      // ignore
    }
  });
};

if (ledgerEnabled && !DISABLE_LEDGER_HYDRATION) {
  await ensureLedgerSchema();
  await seedManagedAgentApiKeys();
  await hydrateLedgerState();
  await hydrateReplayStreamState();
} else if (!ledgerEnabled) {
  console.warn(
    `[prediction-markets] databaseUrl is not set in ${APP_CONFIG.__source ?? "local config"}; round persistence is disabled`,
  );
} else {
  await ensureLedgerSchema();
  await seedManagedAgentApiKeys();
}
if (!DISABLE_SECOND_TIMER) {
  ensureSecondTimer();
}
ensureNonceRelayTimer();
if (!RECONCILE_ONCE && shouldRunChainReconciliation()) {
  await runChainReconciliationPass();
  ensureReconciliationTimer();
}
if (RECONCILE_ONCE) {
  await runChainReconciliationPass({
    force: true,
    fromBlockOverride: RECONCILE_FROM_BLOCK,
    toBlockOverride: RECONCILE_TO_BLOCK,
    updateCursors: RECONCILE_UPDATE_CURSORS,
  });
  console.log(
    `[prediction-markets] reconciliation complete (from=${RECONCILE_FROM_BLOCK ?? "cursor"}, to=${RECONCILE_TO_BLOCK ?? "latest"}, updateCursors=${RECONCILE_UPDATE_CURSORS})`,
  );
  process.exit(0);
}
if (!DISABLE_UPSTREAM_FEED) {
  connectUpstream();
}
await processMonitoringTick();
ensureMonitoringTimer();

const server = Bun.serve({
  port: PORT,
  async fetch(req, serverInstance) {
    const url = new URL(req.url);
    const authenticateAgent = async requiredScope => {
      const auth = await authenticateAgentRequest(req);
      if (!auth) {
        return {
          response: jsonResponse(401, {
            code: "AUTH_INVALID_API_KEY",
            error: AGENT_ERROR_CODES.AUTH_INVALID_API_KEY.message,
            retryable: AGENT_ERROR_CODES.AUTH_INVALID_API_KEY.retryable,
          }),
        };
      }
      if (!auth.ok) {
        return {
          response: jsonResponse(auth.status ?? 401, {
            code: auth.code ?? null,
            error: auth.error,
            retryable: auth.retryable ?? false,
          }),
        };
      }
      if (!requiredScope) {
        return { agent: auth.agent };
      }
      const authorization = requireAgentScope(auth.agent, requiredScope);
      if (!authorization.ok) {
        return {
          response: jsonResponse(authorization.status, {
            code: authorization.code ?? null,
            error: authorization.error,
            retryable: authorization.retryable ?? false,
          }),
        };
      }
      return { agent: auth.agent };
    };

    if (
      url.pathname === "/llms.txt" ||
      url.pathname === `${BASE_PATH}/llms.txt`
    ) {
      return new Response(buildLlmsTxt(), {
        headers: { "Content-Type": "text/plain; charset=utf-8" },
      });
    }

    if (url.pathname === "/api/agent/protocol" || url.pathname === AGENT_PROTOCOL_SCHEMA_PATH) {
      return jsonResponse(200, buildAgentProtocolDescriptor());
    }

    if (url.pathname === "/api/agent/schemas" || url.pathname === AGENT_PROTOCOL_SCHEMAS_PATH) {
      return jsonResponse(200, buildAgentProtocolSchemas());
    }

    if (url.pathname === "/api/status" || url.pathname === `${BASE_PATH}/api/status`) {
      const monitoring = await buildRuntimeMonitoringSnapshot();
      const alerts = evaluateMonitoringAlerts(monitoring);
      return jsonResponse(200, {
        status: "ok",
        app: "prediction-markets",
        protocol_version: AGENT_PROTOCOL_VERSION,
        port: PORT,
        base_path: BASE_PATH,
        config_source: APP_CONFIG.__source,
        ledger_enabled: ledgerEnabled,
        feed_connected: state.feedConnected,
        client_count: publicClients.size + agentClients.size,
        public_client_count: publicClients.size,
        agent_client_count: agentClients.size,
        last_price: state.lastPrice,
        quote: state.quote,
        last_trade_time_ms: state.lastTrade?.timeMs ?? null,
        nonce_relay_mode: NONCE_RELAY_MODE,
        fill_settlement_mode: FILL_SETTLEMENT_MODE,
        settlement_mode: SETTLEMENT_MODE,
        settlement_manager_address: SETTLEMENT_MANAGER_ADDRESS,
        reconciliation_enabled: shouldRunChainReconciliation(),
        reconciliation_poll_ms: RECONCILIATION_POLL_MS,
        monitoring: {
          poll_ms: MONITORING_POLL_MS,
          webhook_enabled: Boolean(MONITORING_WEBHOOK_URL),
          alerts,
          snapshot: monitoring,
        },
      });
    }

    if (url.pathname === "/api/me" || url.pathname === `${BASE_PATH}/api/me`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      return jsonResponse(200, await buildAgentMePayload(authenticated.agent));
    }

    if (url.pathname === "/api/account/summary" || url.pathname === `${BASE_PATH}/api/account/summary`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      return jsonResponse(200, await buildAgentAccountSummary(authenticated.agent));
    }

    if (url.pathname === "/api/notifications" || url.pathname === `${BASE_PATH}/api/notifications`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      if (req.method === "GET") {
        return jsonResponse(200, {
          agent_id: authenticated.agent.agentId,
          items: await listAgentNotifications({ agentId: authenticated.agent.agentId }),
        });
      }
      if (req.method === "POST") {
        const body = await req.json().catch(() => ({}));
        const result = await createAgentNotification({
          agentId: authenticated.agent.agentId,
          provider: body?.provider,
          label: body?.label ?? null,
          webhookUrl: body?.webhook_url ?? null,
          telegramBotToken: body?.bot_token ?? null,
          telegramChatId: body?.chat_id ?? null,
          events: body?.events,
        });
        if (!result.ok) {
          return jsonResponse(result.status ?? 400, { error: result.error });
        }
        return jsonResponse(200, result);
      }
      return jsonResponse(405, { error: "Method not allowed." });
    }

    if (
      url.pathname.startsWith("/api/notifications/") ||
      url.pathname.startsWith(`${BASE_PATH}/api/notifications/`)
    ) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      const prefix = url.pathname.startsWith(BASE_PATH) ? `${BASE_PATH}/api/notifications/` : "/api/notifications/";
      const id = decodeURIComponent(url.pathname.slice(prefix.length));
      if (!id) {
        return jsonResponse(400, { error: "id is required." });
      }
      if (req.method === "PATCH") {
        const body = await req.json().catch(() => ({}));
        const result = await updateAgentNotification({
          agentId: authenticated.agent.agentId,
          id,
          label: body?.label,
          status: body?.status,
          events: body?.events,
        });
        if (!result.ok) {
          return jsonResponse(result.status ?? 400, { error: result.error });
        }
        return jsonResponse(200, result);
      }
      if (req.method === "DELETE") {
        const result = await deleteAgentNotification({
          agentId: authenticated.agent.agentId,
          id,
        });
        if (!result.ok) {
          return jsonResponse(result.status ?? 400, { error: result.error });
        }
        return jsonResponse(200, result);
      }
      return jsonResponse(405, { error: "Method not allowed." });
    }

    if (url.pathname === "/api/markets" || url.pathname === `${BASE_PATH}/api/markets`) {
      return jsonResponse(200, await listMarketsDirectory());
    }

    if (url.pathname === "/api/streams/backfill" || url.pathname === `${BASE_PATH}/api/streams/backfill`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      const streamKey = url.searchParams.get("stream");
      const parsedStream = parseStreamKey(streamKey);
      if (!parsedStream) {
        return jsonResponse(400, { error: "stream is required." });
      }
      if (parsedStream.kind === "positions" || parsedStream.kind === "nonce_invalidations") {
        if (parsedStream.agentId !== authenticated.agent.agentId) {
          return jsonResponse(403, { error: "stream does not belong to this agent." });
        }
      }
      if (parsedStream.kind === "nonce_invalidations") {
        const authorization = requireAgentScope(authenticated.agent, "nonce_invalidations:read");
        if (!authorization.ok) {
          return jsonResponse(authorization.status, { code: authorization.code ?? null, error: authorization.error, retryable: authorization.retryable ?? false });
        }
      }
      if (!["book", "positions", "nonce_invalidations"].includes(parsedStream.kind)) {
        return jsonResponse(400, { error: "unsupported stream kind." });
      }
      const items = await listPersistedStreamEvents({
        streamKey,
        sinceSequence: url.searchParams.get("since_sequence"),
        limit: url.searchParams.get("limit"),
      });
      const stream = ensureAgentStream(streamKey);
      return jsonResponse(200, {
        stream: streamKey,
        current_sequence: stream.sequence,
        oldest_persisted_sequence: items[0]?.sequence ?? streamOldestReplayableSequence(streamKey),
        items,
      });
    }

    if (url.pathname === "/api/markets/current" || url.pathname === `${BASE_PATH}/api/markets/current`) {
      const marketId = getCurrentMarketId(state.currentRound);
      return jsonResponse(200, {
        market_id: marketId,
        current_round: state.currentRound,
        previous_round: state.previousRound,
        feed_connected: state.feedConnected,
        last_price: state.lastPrice,
        quote: state.quote,
      });
    }

    if (url.pathname === "/api/book" || url.pathname === `${BASE_PATH}/api/book`) {
      const marketId = url.searchParams.get("market_id") || getCurrentMarketId(state.currentRound);
      if (!marketId) {
        return jsonResponse(404, { error: "No active market." });
      }
      return jsonResponse(200, serializeBook(marketId));
    }

    if (url.pathname === "/api/resolutions" || url.pathname === `${BASE_PATH}/api/resolutions`) {
      const cursorRaw = url.searchParams.get("cursor");
      const limitRaw = url.searchParams.get("limit");
      const cursorStartMs = cursorRaw === null ? null : Number(cursorRaw);
      const limit = limitRaw === null ? RESOLUTION_PAGE_SIZE_DEFAULT : Number(limitRaw);
      const page = await listResolvedRoundsPage({
        cursorStartMs: Number.isFinite(cursorStartMs) ? cursorStartMs : null,
        limit,
      });
      return jsonResponse(200, {
        items: page.items,
        next_cursor: page.nextCursor,
      });
    }

    if (url.pathname === "/api/admin/agent-keys" || url.pathname === `${BASE_PATH}/api/admin/agent-keys`) {
      const authenticated = await authenticateAgent("admin");
      if (authenticated.response) return authenticated.response;
      if (req.method === "GET") {
        const items = await listManagedAgentApiKeys({
          agentId: url.searchParams.get("agent_id") || null,
          includeInactive: url.searchParams.get("include_inactive") === "true",
        });
        return jsonResponse(200, {
          items,
        });
      }
      if (req.method === "POST") {
        const body = await req.json().catch(() => ({}));
        const result = await createManagedAgentApiKey({
          agentId: body?.agent_id ?? null,
          label: body?.label ?? null,
          scopes: body?.scopes,
          collateralUsd: body?.collateral_usd,
          rateLimitPerMinute: body?.rate_limit_per_minute,
        });
        if (!result.ok) {
          return jsonResponse(result.status ?? 400, { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false });
        }
        return jsonResponse(200, result);
      }
      return jsonResponse(405, { error: "Method not allowed." });
    }

    if (
      url.pathname.startsWith("/api/admin/agent-keys/") ||
      url.pathname.startsWith(`${BASE_PATH}/api/admin/agent-keys/`)
    ) {
      const authenticated = await authenticateAgent("admin");
      if (authenticated.response) return authenticated.response;
      const prefix = url.pathname.startsWith(BASE_PATH) ? `${BASE_PATH}/api/admin/agent-keys/` : "/api/admin/agent-keys/";
      const keyId = decodeURIComponent(url.pathname.slice(prefix.length));
      if (!keyId) {
        return jsonResponse(400, { error: "key_id is required." });
      }
      if (req.method === "PATCH") {
        const body = await req.json().catch(() => ({}));
        const result = await updateManagedAgentApiKey({
          keyId,
          label: body?.label,
          scopes: body?.scopes,
          collateralUsd: body?.collateral_usd,
          rateLimitPerMinute: body?.rate_limit_per_minute,
          status: body?.status,
        });
        if (!result.ok) {
          return jsonResponse(result.status ?? (result.error === "API key not found." ? 404 : 400), {
            code: result.code ?? null,
            error: result.error,
            retryable: result.retryable ?? false,
          });
        }
        return jsonResponse(200, result);
      }
      if (req.method === "DELETE") {
        const result = await revokeManagedAgentApiKey(keyId);
        if (!result.ok) {
          return jsonResponse(result.status ?? (result.error === "API key not found." ? 404 : 400), {
            code: result.code ?? null,
            error: result.error,
            retryable: result.retryable ?? false,
          });
        }
        return jsonResponse(200, result);
      }
      return jsonResponse(405, { error: "Method not allowed." });
    }

    if (url.pathname === "/api/orders" || url.pathname === `${BASE_PATH}/api/orders`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      const agent = authenticated.agent;
      if (req.method === "GET") {
        const marketId = url.searchParams.get("market_id");
        const orders = await listAgentOrders({
          agentId: agent.agentId,
          marketId,
          limit: Number(url.searchParams.get("limit") ?? ORDER_PAGE_SIZE_DEFAULT),
        });
        return jsonResponse(200, {
          agent_id: agent.agentId,
          items: orders,
        });
      }
      return jsonResponse(405, { error: "Method not allowed." });
    }

    if (url.pathname === "/api/orders/place" || url.pathname === `${BASE_PATH}/api/orders/place`) {
      const authenticated = await authenticateAgent("orders:write");
      if (authenticated.response) return authenticated.response;
      if (req.method !== "POST") {
        return jsonResponse(405, { error: "Method not allowed." });
      }
      const body = await req.json().catch(() => ({}));
      const result = await placeOrder({
        agent: authenticated.agent,
        payload: body ?? {},
      });
      if (!result.ok) {
        return jsonResponse(result.status ?? 400, { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false });
      }
      return jsonResponse(200, { order: result.order });
    }

    if (url.pathname === "/api/orders/cancel" || url.pathname === `${BASE_PATH}/api/orders/cancel`) {
      const authenticated = await authenticateAgent("orders:cancel");
      if (authenticated.response) return authenticated.response;
      if (req.method !== "POST") {
        return jsonResponse(405, { error: "Method not allowed." });
      }
      const body = await req.json().catch(() => ({}));
      const result = await cancelOrder({
        agent: authenticated.agent,
        orderHash: body?.order_hash ?? null,
      });
      if (!result.ok) {
        return jsonResponse(result.status ?? 400, { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false });
      }
      return jsonResponse(200, result);
    }

    if (url.pathname === "/api/orders/take" || url.pathname === `${BASE_PATH}/api/orders/take`) {
      const authenticated = await authenticateAgent("fills:take");
      if (authenticated.response) return authenticated.response;
      if (req.method !== "POST") {
        return jsonResponse(405, { error: "Method not allowed." });
      }
      const body = await req.json().catch(() => ({}));
      const result = await takeOrder({
        agent: authenticated.agent,
        payload: body ?? {},
      });
      if (!result.ok) {
        return jsonResponse(result.status ?? 400, { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false });
      }
      return jsonResponse(200, { fill: result.fill });
    }

    if (
      url.pathname.startsWith("/api/orders/") ||
      url.pathname.startsWith(`${BASE_PATH}/api/orders/`)
    ) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      if (req.method !== "GET") {
        return jsonResponse(405, { error: "Method not allowed." });
      }
      const prefix = url.pathname.startsWith(BASE_PATH) ? `${BASE_PATH}/api/orders/` : "/api/orders/";
      const orderHash = decodeURIComponent(url.pathname.slice(prefix.length));
      if (!orderHash || ["place", "cancel", "take"].includes(orderHash)) {
        return jsonResponse(404, { error: "Not found." });
      }
      const order = await getAgentOrderByHash({
        agentId: authenticated.agent.agentId,
        orderHash,
      });
      if (!order) {
        return jsonResponse(404, {
          code: "ORDER_NOT_FOUND",
          error: AGENT_ERROR_CODES.ORDER_NOT_FOUND.message,
          retryable: false,
        });
      }
      return jsonResponse(200, {
        agent_id: authenticated.agent.agentId,
        order,
      });
    }

    if (url.pathname === "/api/order-events" || url.pathname === `${BASE_PATH}/api/order-events`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      const agent = authenticated.agent;
      const marketId = url.searchParams.get("market_id");
      const items = await listAgentOrderEvents({
        agentId: agent.agentId,
        marketId,
        limit: Number(url.searchParams.get("limit") ?? ORDER_PAGE_SIZE_DEFAULT),
      });
      return jsonResponse(200, {
        agent_id: agent.agentId,
        items,
      });
    }

    if (url.pathname === "/api/fills" || url.pathname === `${BASE_PATH}/api/fills`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      const agent = authenticated.agent;
      const marketId = url.searchParams.get("market_id");
      const items = await listAgentFills({
        agentId: agent.agentId,
        marketId,
        limit: Number(url.searchParams.get("limit") ?? FILL_PAGE_SIZE_DEFAULT),
      });
      return jsonResponse(200, {
        agent_id: agent.agentId,
        items,
      });
    }

    if (url.pathname === "/api/claims" || url.pathname === `${BASE_PATH}/api/claims`) {
      const authenticated = await authenticateAgent(req.method === "POST" ? "claims:write" : "market:read");
      if (authenticated.response) return authenticated.response;
      const agent = authenticated.agent;
      if (req.method === "GET") {
        const marketId = url.searchParams.get("market_id");
        const items = await listAgentClaims({
          agentId: agent.agentId,
          marketId,
          limit: Number(url.searchParams.get("limit") ?? ORDER_PAGE_SIZE_DEFAULT),
        });
        return jsonResponse(200, {
          agent_id: agent.agentId,
          items,
        });
      }
      if (req.method === "POST") {
        const body = await req.json().catch(() => ({}));
        const result = await submitClaim({
          agent,
          marketId: body?.market_id ?? null,
          claimantAddress: body?.claimant_address ?? null,
        });
        if (!result.ok) {
          return jsonResponse(result.status ?? 400, { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false });
        }
        return jsonResponse(200, result);
      }
      return jsonResponse(405, { error: "Method not allowed." });
    }

    if (url.pathname === "/api/nonce-invalidations" || url.pathname === `${BASE_PATH}/api/nonce-invalidations`) {
      const authenticated = await authenticateAgent("nonce_invalidations:read");
      if (authenticated.response) return authenticated.response;
      const agent = authenticated.agent;
      const marketId = url.searchParams.get("market_id");
      const items = await listAgentNonceInvalidations({
        agentId: agent.agentId,
        marketId,
        limit: Number(url.searchParams.get("limit") ?? ORDER_PAGE_SIZE_DEFAULT),
      });
      return jsonResponse(200, {
        agent_id: agent.agentId,
        items,
      });
    }

    if (url.pathname === "/api/positions" || url.pathname === `${BASE_PATH}/api/positions`) {
      const authenticated = await authenticateAgent("market:read");
      if (authenticated.response) return authenticated.response;
      const agent = authenticated.agent;
      const marketId = url.searchParams.get("market_id");
      const items = await listAgentPositions({
        agentId: agent.agentId,
        marketId,
      });
      return jsonResponse(200, {
        agent_id: agent.agentId,
        items,
      });
    }

    if (url.pathname === "/ws" || url.pathname === `${BASE_PATH}/ws`) {
      if (serverInstance.upgrade(req, { data: { kind: "public" } })) {
        return;
      }
      return new Response("WebSocket upgrade failed", { status: 500 });
    }

    if (url.pathname === "/ws/agent" || url.pathname === `${BASE_PATH}/ws/agent`) {
      const auth = await authenticateAgentRequest(req);
      if (!auth) {
        return new Response("Unauthorized", { status: 401 });
      }
      if (!auth.ok) {
        return new Response(auth.error, { status: auth.status ?? 401 });
      }
      const readAuthorization = requireAgentScope(auth.agent, "market:read");
      if (!readAuthorization.ok) {
        return new Response(readAuthorization.error, { status: readAuthorization.status });
      }
      if (serverInstance.upgrade(req, {
        data: {
          kind: "agent",
          agent: auth.agent,
          subscriptions: {
            books: new Set(),
          },
        },
      })) {
        return;
      }
      return new Response("WebSocket upgrade failed", { status: 500 });
    }

    const filePath = resolvePublicFile(url.pathname);
    if (!filePath || !existsSync(filePath)) {
      return new Response("Not found", { status: 404 });
    }

    const file = Bun.file(filePath);
    return new Response(file, {
      headers: {
        "Content-Type": MIME_TYPES[extname(filePath)] || file.type || "application/octet-stream",
      },
    });
  },
  websocket: {
    open(ws) {
      if (ws.data?.kind === "agent") {
        agentClients.add(ws);
        try {
          const marketId = getCurrentMarketId(state.currentRound);
          if (marketId) {
            ws.data.subscriptions.books.add(marketId);
          }
          ws.send(JSON.stringify(buildAgentBootstrapPayload(ws)));
          void sendAgentPositionsSnapshot(ws);
        } catch {
          agentClients.delete(ws);
        }
        return;
      }
      publicClients.add(ws);
      try {
        ws.send(JSON.stringify(buildBootstrapPayload()));
      } catch {
        publicClients.delete(ws);
      }
    },
    close(ws) {
      if (ws.data?.kind === "agent") {
        agentClients.delete(ws);
        recordWebsocketDisconnect("agent");
        return;
      }
      publicClients.delete(ws);
      recordWebsocketDisconnect("public");
    },
    async message(ws, message) {
      if (typeof message === "string" && message === "ping") {
        ws.send("pong");
        return;
      }
      if (ws.data?.kind !== "agent" || typeof message !== "string") {
        return;
      }
      const rateLimitCheck = enforceAgentRateLimit(ws.data?.agent);
      if (!rateLimitCheck.ok) {
        ws.send(JSON.stringify({ type: "error", payload: { error: rateLimitCheck.error } }));
        return;
      }
      recordAgentAuthUsage(ws.data?.agent);
      let parsed;
      try {
        parsed = JSON.parse(message);
      } catch {
        ws.send(JSON.stringify({ type: "error", payload: { error: "Invalid JSON." } }));
        return;
      }
      const ensureScope = scope => {
        const authorization = requireAgentScope(ws.data?.agent, scope);
        if (!authorization.ok) {
          ws.send(JSON.stringify({ type: "error", payload: { error: authorization.error } }));
          return false;
        }
        return true;
      };
      const type = parsed?.type;
      if (type === "book.subscribe") {
        if (!ensureScope("market:read")) return;
        const marketId = parsed?.payload?.market_id || getCurrentMarketId(state.currentRound);
        await subscribeBookStream({
          ws,
          marketId,
          sinceSequence: parsed?.payload?.since_sequence ?? null,
        });
        return;
      }
      if (type === "positions.subscribe") {
        if (!ensureScope("market:read")) return;
        await subscribePositionsStream({
          ws,
          marketId: parsed?.payload?.market_id ?? getCurrentMarketId(state.currentRound),
          sinceSequence: parsed?.payload?.since_sequence ?? null,
        });
        return;
      }
      if (type === "nonce_invalidations.subscribe") {
        if (!ensureScope("nonce_invalidations:read")) return;
        await subscribeNonceInvalidationsStream({
          ws,
          marketId: parsed?.payload?.market_id ?? getCurrentMarketId(state.currentRound),
          sinceSequence: parsed?.payload?.since_sequence ?? null,
        });
        return;
      }
      if (type === "order.place") {
        if (!ensureScope("orders:write")) return;
        const result = await placeOrder({
          agent: ws.data.agent,
          payload: parsed?.payload ?? {},
        });
        if (!result.ok) {
          ws.send(JSON.stringify({ type: "order.reject", payload: { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false } }));
          return;
        }
        ws.send(JSON.stringify({ type: "order.ack", payload: { order: result.order } }));
        return;
      }
      if (type === "order.cancel") {
        if (!ensureScope("orders:cancel")) return;
        const result = await cancelOrder({
          agent: ws.data.agent,
          orderHash: parsed?.payload?.order_hash ?? null,
        });
        if (!result.ok) {
          ws.send(JSON.stringify({ type: "order.reject", payload: { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false } }));
          return;
        }
        ws.send(JSON.stringify({ type: "order.cancelled", payload: result }));
        return;
      }
      if (type === "order.take") {
        if (!ensureScope("fills:take")) return;
        const result = await takeOrder({
          agent: ws.data.agent,
          payload: parsed?.payload ?? {},
        });
        if (!result.ok) {
          ws.send(JSON.stringify({ type: "order.reject", payload: { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false } }));
          return;
        }
        ws.send(JSON.stringify({ type: "order.fill_ack", payload: result.fill }));
        return;
      }
      if (type === "claim.submit") {
        if (!ensureScope("claims:write")) return;
        const result = await submitClaim({
          agent: ws.data.agent,
          marketId: parsed?.payload?.market_id ?? null,
          claimantAddress: parsed?.payload?.claimant_address ?? null,
        });
        if (!result.ok) {
          ws.send(JSON.stringify({ type: "claim.reject", payload: { code: result.code ?? null, error: result.error, retryable: result.retryable ?? false } }));
          return;
        }
        ws.send(JSON.stringify({ type: "claim.ack", payload: result.claim }));
        return;
      }
    },
  },
});

console.log(`Prediction Markets running at http://localhost:${server.port}`);
