// src/agent-client.ts
async function requestJsonUnauthenticated(baseUrl, path, fetchImpl = fetch, init = {}) {
  const response = await fetchImpl(`${baseUrl.replace(/\/+$/, "")}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...init.headers || {}
    }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `${path} failed with ${response.status}`);
  }
  return payload;
}
function requestWalletAuthChallenge(baseUrl, walletAddress, fetchImpl) {
  return requestJsonUnauthenticated(baseUrl, "/api/auth/challenge", fetchImpl, {
    method: "POST",
    body: JSON.stringify({ wallet_address: walletAddress })
  });
}
function issueWalletAuthKey(baseUrl, payload, fetchImpl) {
  return requestJsonUnauthenticated(baseUrl, "/api/auth/issue-key", fetchImpl, {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

class PredictionMarketsAgentClient {
  baseUrl;
  apiKey;
  reconnect;
  reconnectDelayMs;
  WebSocketImpl;
  fetchImpl;
  ws = null;
  reconnectTimer = null;
  manuallyClosed = false;
  onOpen = null;
  onClose = null;
  onError = null;
  onMessage = null;
  onRecoveryRequired = null;
  constructor(options) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.apiKey = options.apiKey;
    this.reconnect = options.reconnect ?? true;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1000;
    this.WebSocketImpl = options.WebSocketImpl ?? WebSocket;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }
  authHeaders() {
    return {
      Authorization: `Bearer ${this.apiKey}`,
      "Content-Type": "application/json"
    };
  }
  async requestJson(path, init = {}) {
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      ...init,
      headers: {
        ...this.authHeaders(),
        ...init.headers || {}
      }
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || `${path} failed with ${response.status}`);
    }
    return payload;
  }
  getStatus() {
    return this.requestJson("/api/status");
  }
  requestWalletAuthChallenge(walletAddress) {
    return requestWalletAuthChallenge(this.baseUrl, walletAddress, this.fetchImpl);
  }
  issueWalletAuthKey(payload) {
    return issueWalletAuthKey(this.baseUrl, payload, this.fetchImpl);
  }
  requestFaucet() {
    return this.requestJson("/api/faucet/request", {
      method: "POST"
    });
  }
  getFaucetStatus() {
    return this.requestJson("/api/faucet/status");
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
  createNotification(payload) {
    return this.requestJson("/api/notifications", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }
  updateNotification(id, payload) {
    return this.requestJson(`/api/notifications/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    });
  }
  deleteNotification(id) {
    return this.requestJson(`/api/notifications/${encodeURIComponent(id)}`, {
      method: "DELETE"
    });
  }
  getCurrentMarket() {
    return this.requestJson("/api/markets/current");
  }
  getOrders() {
    return this.requestJson("/api/orders");
  }
  getOrder(orderHash) {
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
  submitClaim(marketId) {
    return this.requestJson("/api/claims", {
      method: "POST",
      body: JSON.stringify({ market_id: marketId })
    });
  }
  placeOrderRest(payload) {
    return this.requestJson("/api/orders/place", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }
  cancelOrderRest(orderHash) {
    return this.requestJson("/api/orders/cancel", {
      method: "POST",
      body: JSON.stringify({ order_hash: orderHash })
    });
  }
  takeOrderRest(orderHash, sizeShares, takerAddress) {
    return this.requestJson("/api/orders/take", {
      method: "POST",
      body: JSON.stringify({
        order_hash: orderHash,
        size_shares: sizeShares,
        ...takerAddress ? { taker_address: takerAddress } : {}
      })
    });
  }
  buildAgentWsUrl() {
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
    this.ws.addEventListener("error", (event) => {
      this.onError?.(event);
    });
    this.ws.addEventListener("message", (event) => {
      const message = JSON.parse(typeof event.data === "string" ? event.data : String(event.data ?? ""));
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
  isConnected() {
    return this.ws?.readyState === this.WebSocketImpl.OPEN;
  }
  send(type, payload = {}) {
    if (!this.ws || this.ws.readyState !== this.WebSocketImpl.OPEN) {
      throw new Error("Agent websocket is not connected.");
    }
    this.ws.send(JSON.stringify({ type, payload }));
  }
  subscribeBook(options = {}) {
    this.send("book.subscribe", {
      market_id: options.marketId ?? null,
      since_sequence: options.sinceSequence ?? null
    });
  }
  subscribePositions(options = {}) {
    this.send("positions.subscribe", {
      market_id: options.marketId ?? null,
      since_sequence: options.sinceSequence ?? null
    });
  }
  subscribeNonceInvalidations(options = {}) {
    this.send("nonce_invalidations.subscribe", {
      market_id: options.marketId ?? null,
      since_sequence: options.sinceSequence ?? null
    });
  }
  placeOrder(payload) {
    this.send("order.place", payload);
  }
  cancelOrder(orderHash) {
    this.send("order.cancel", { order_hash: orderHash });
  }
  takeOrder(orderHash, sizeShares, takerAddress) {
    this.send("order.take", {
      order_hash: orderHash,
      size_shares: sizeShares,
      ...takerAddress ? { taker_address: takerAddress } : {}
    });
  }
  clearReconnectTimer() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
export {
  requestWalletAuthChallenge,
  issueWalletAuthKey,
  PredictionMarketsAgentClient
};
