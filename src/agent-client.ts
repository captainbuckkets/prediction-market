export type Json = Record<string, unknown>;

export type AgentScope =
  | "market:read"
  | "orders:write"
  | "orders:cancel"
  | "fills:take"
  | "claims:write"
  | "nonce_invalidations:read"
  | "admin"
  | "*";

export type AgentStreamMode = "snapshot" | "replay" | "snapshot_after_gap";
export type AgentRecoveryReason = "future_sequence" | "gap_out_of_range";

export type AgentStreamSync = {
  stream: string;
  mode: AgentStreamMode;
  from_sequence: number | null;
  to_sequence: number | null;
  server_now_ms: number;
};

export type AgentStreamRecoveryRequired = {
  stream: string;
  requested_sequence: number;
  latest_sequence: number;
  oldest_replayable_sequence: number;
  reason: AgentRecoveryReason;
  server_now_ms: number;
};

export type AgentBootstrapPayload = {
  server_now_ms: number;
  agent_id: string | null;
  market_id: string | null;
  current_round: { endMs?: number } | null;
  streams?: Json;
  auth?: {
    key_id?: string | null;
    key_prefix?: string | null;
    source?: string | null;
    scopes?: AgentScope[];
    rate_limit_per_minute?: number | null;
  };
  capabilities?: Json;
} & Json;

export type AgentWsMessage =
  | { type: "agent.bootstrap"; payload: AgentBootstrapPayload }
  | { type: "stream.sync"; payload: AgentStreamSync }
  | { type: "stream.recovery_required"; payload: AgentStreamRecoveryRequired }
  | { type: "error"; payload: { error?: string } & Json }
  | { type: string; payload?: Json };

export type PredictionMarketsAgentClientOptions = {
  baseUrl: string;
  apiKey: string;
  reconnect?: boolean;
  reconnectDelayMs?: number;
  WebSocketImpl?: typeof WebSocket;
  fetchImpl?: typeof fetch;
};

export type SubscribeStreamOptions = {
  marketId?: string | null;
  sinceSequence?: number | null;
};

export type PlaceOrderPayload = {
  market_id: string;
  side: "buy_yes" | "sell_yes";
  price_tenths: number;
  size_shares: number;
  expiry_ms?: number;
  client_order_id?: string;
  maker_address?: string;
  nonce?: number;
  signature?: string;
};

export class PredictionMarketsAgentClient {
  readonly baseUrl: string;
  readonly apiKey: string;
  readonly reconnect: boolean;
  readonly reconnectDelayMs: number;
  readonly WebSocketImpl: typeof WebSocket;
  readonly fetchImpl: typeof fetch;

  private ws: WebSocket | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private manuallyClosed = false;

  onOpen: (() => void) | null = null;
  onClose: (() => void) | null = null;
  onError: ((error: unknown) => void) | null = null;
  onMessage: ((message: AgentWsMessage) => void) | null = null;
  onRecoveryRequired: ((payload: AgentStreamRecoveryRequired) => void) | null = null;

  constructor(options: PredictionMarketsAgentClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.apiKey = options.apiKey;
    this.reconnect = options.reconnect ?? true;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1000;
    this.WebSocketImpl = options.WebSocketImpl ?? WebSocket;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  authHeaders(): Record<string, string> {
    return {
      Authorization: `Bearer ${this.apiKey}`,
      "Content-Type": "application/json",
    };
  }

  async requestJson<T = Json>(path: string, init: RequestInit = {}): Promise<T> {
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      ...init,
      headers: {
        ...this.authHeaders(),
        ...(init.headers || {}),
      },
    });
    const payload = (await response.json().catch(() => ({}))) as T & { error?: string };
    if (!response.ok) {
      throw new Error(payload.error || `${path} failed with ${response.status}`);
    }
    return payload;
  }

  getStatus() {
    return this.requestJson("/api/status");
  }

  getProtocol() {
    return this.requestJson("/api/agent/protocol");
  }

  getSchemas() {
    return this.requestJson("/api/agent/schemas");
  }

  getMe() {
    return this.requestJson("/api/me");
  }

  getAccountSummary() {
    return this.requestJson("/api/account/summary");
  }

  getMarkets() {
    return this.requestJson("/api/markets");
  }

  getNotifications() {
    return this.requestJson("/api/notifications");
  }

  createNotification(payload: Record<string, unknown>) {
    return this.requestJson("/api/notifications", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  }

  updateNotification(id: string, payload: Record<string, unknown>) {
    return this.requestJson(`/api/notifications/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
  }

  deleteNotification(id: string) {
    return this.requestJson(`/api/notifications/${encodeURIComponent(id)}`, {
      method: "DELETE",
    });
  }

  getCurrentMarket() {
    return this.requestJson<{ market_id: string | null }>("/api/markets/current");
  }

  getOrders() {
    return this.requestJson("/api/orders");
  }

  getOrder(orderHash: string) {
    return this.requestJson(`/api/orders/${encodeURIComponent(orderHash)}`);
  }

  getOrderEvents() {
    return this.requestJson("/api/order-events");
  }

  getFills() {
    return this.requestJson("/api/fills");
  }

  getPositions() {
    return this.requestJson("/api/positions");
  }

  getClaims() {
    return this.requestJson("/api/claims");
  }

  getNonceInvalidations() {
    return this.requestJson("/api/nonce-invalidations");
  }

  submitClaim(marketId: string) {
    return this.requestJson("/api/claims", {
      method: "POST",
      body: JSON.stringify({ market_id: marketId }),
    });
  }

  placeOrderRest(payload: PlaceOrderPayload) {
    return this.requestJson("/api/orders/place", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  }

  cancelOrderRest(orderHash: string) {
    return this.requestJson("/api/orders/cancel", {
      method: "POST",
      body: JSON.stringify({ order_hash: orderHash }),
    });
  }

  takeOrderRest(orderHash: string, sizeShares: number, takerAddress?: string) {
    return this.requestJson("/api/orders/take", {
      method: "POST",
      body: JSON.stringify({
        order_hash: orderHash,
        size_shares: sizeShares,
        ...(takerAddress ? { taker_address: takerAddress } : {}),
      }),
    });
  }

  buildAgentWsUrl(): string {
    const wsBase = this.baseUrl.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
    const url = new URL(`${wsBase}/ws/agent`);
    url.searchParams.set("api_key", this.apiKey);
    return url.toString();
  }

  connect() {
    this.manuallyClosed = false;
    this.clearReconnectTimer();
    this.ws = new this.WebSocketImpl(this.buildAgentWsUrl());

    this.ws.addEventListener("open", () => {
      this.onOpen?.();
    });
    this.ws.addEventListener("close", () => {
      this.onClose?.();
      if (this.reconnect && !this.manuallyClosed) {
        this.reconnectTimer = setTimeout(() => this.connect(), this.reconnectDelayMs);
      }
    });
    this.ws.addEventListener("error", event => {
      this.onError?.(event);
    });
    this.ws.addEventListener("message", event => {
      const message = JSON.parse(
        typeof event.data === "string" ? event.data : String(event.data ?? ""),
      ) as AgentWsMessage;
      if (message.type === "stream.recovery_required") {
        this.onRecoveryRequired?.(message.payload);
      }
      this.onMessage?.(message);
    });
  }

  disconnect() {
    this.manuallyClosed = true;
    this.clearReconnectTimer();
    if (this.ws) {
      try {
        this.ws.close();
      } catch {}
    }
    this.ws = null;
  }

  isConnected(): boolean {
    return this.ws?.readyState === this.WebSocketImpl.OPEN;
  }

  send(type: string, payload: Json = {}) {
    if (!this.ws || this.ws.readyState !== this.WebSocketImpl.OPEN) {
      throw new Error("Agent websocket is not connected.");
    }
    this.ws.send(JSON.stringify({ type, payload }));
  }

  subscribeBook(options: SubscribeStreamOptions = {}) {
    this.send("book.subscribe", {
      market_id: options.marketId ?? null,
      since_sequence: options.sinceSequence ?? null,
    });
  }

  subscribePositions(options: SubscribeStreamOptions = {}) {
    this.send("positions.subscribe", {
      market_id: options.marketId ?? null,
      since_sequence: options.sinceSequence ?? null,
    });
  }

  subscribeNonceInvalidations(options: SubscribeStreamOptions = {}) {
    this.send("nonce_invalidations.subscribe", {
      market_id: options.marketId ?? null,
      since_sequence: options.sinceSequence ?? null,
    });
  }

  placeOrder(payload: PlaceOrderPayload) {
    this.send("order.place", payload as unknown as Json);
  }

  cancelOrder(orderHash: string) {
    this.send("order.cancel", { order_hash: orderHash });
  }

  takeOrder(orderHash: string, sizeShares: number, takerAddress?: string) {
    this.send("order.take", {
      order_hash: orderHash,
      size_shares: sizeShares,
      ...(takerAddress ? { taker_address: takerAddress } : {}),
    });
  }

  private clearReconnectTimer() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
