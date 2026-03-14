export const DEFAULT_MONITORING_THRESHOLDS = {
  feedStaleMs: 15_000,
  reconciliationStaleMs: 60_000,
  pendingActionStaleMs: 120_000,
  relayWalletMinEth: 0.001,
  onchainFailureLookbackMs: 300_000,
  websocketDisconnectLookbackMs: 60_000,
  websocketDisconnectMaxCount: 25,
};

const toNumberOrNull = value => {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

export const buildMonitoringSnapshot = ({
  nowMs,
  feedConnected,
  lastFeedEventAtMs = null,
  reconciliationEnabled = false,
  lastReconciliationSuccessAtMs = null,
  lastReconciliationFailureAtMs = null,
  lastReconciliationError = null,
  pendingNonceInvalidations = 0,
  oldestPendingNonceCreatedAtMs = null,
  failedNonceInvalidations = 0,
  relayWalletBalanceEth = null,
  recentOnchainFailureCount = 0,
  recentWebsocketDisconnectCount = 0,
  websocketPublicClientCount = 0,
  websocketAgentClientCount = 0,
  thresholds = {},
} = {}) => {
  const mergedThresholds = {
    ...DEFAULT_MONITORING_THRESHOLDS,
    ...(thresholds ?? {}),
  };
  const normalizedNowMs = toNumberOrNull(nowMs) ?? Date.now();
  const normalizedLastFeedEventAtMs = toNumberOrNull(lastFeedEventAtMs);
  const normalizedLastReconciliationSuccessAtMs = toNumberOrNull(lastReconciliationSuccessAtMs);
  const normalizedLastReconciliationFailureAtMs = toNumberOrNull(lastReconciliationFailureAtMs);
  const normalizedOldestPendingNonceCreatedAtMs = toNumberOrNull(oldestPendingNonceCreatedAtMs);
  const normalizedRelayWalletBalanceEth =
    relayWalletBalanceEth === null || relayWalletBalanceEth === undefined ? null : Number(relayWalletBalanceEth);

  return {
    nowMs: normalizedNowMs,
    thresholds: mergedThresholds,
    feedConnected: Boolean(feedConnected),
    lastFeedEventAtMs: normalizedLastFeedEventAtMs,
    lastFeedEventAgeMs:
      normalizedLastFeedEventAtMs === null ? null : Math.max(0, normalizedNowMs - normalizedLastFeedEventAtMs),
    reconciliationEnabled: Boolean(reconciliationEnabled),
    lastReconciliationSuccessAtMs: normalizedLastReconciliationSuccessAtMs,
    lastReconciliationSuccessAgeMs:
      normalizedLastReconciliationSuccessAtMs === null
        ? null
        : Math.max(0, normalizedNowMs - normalizedLastReconciliationSuccessAtMs),
    lastReconciliationFailureAtMs: normalizedLastReconciliationFailureAtMs,
    lastReconciliationError: lastReconciliationError ?? null,
    pendingNonceInvalidations: Math.max(0, Number(pendingNonceInvalidations) || 0),
    oldestPendingNonceCreatedAtMs: normalizedOldestPendingNonceCreatedAtMs,
    oldestPendingNonceAgeMs:
      normalizedOldestPendingNonceCreatedAtMs === null
        ? null
        : Math.max(0, normalizedNowMs - normalizedOldestPendingNonceCreatedAtMs),
    failedNonceInvalidations: Math.max(0, Number(failedNonceInvalidations) || 0),
    relayWalletBalanceEth:
      normalizedRelayWalletBalanceEth === null || Number.isNaN(normalizedRelayWalletBalanceEth)
        ? null
        : normalizedRelayWalletBalanceEth,
    recentOnchainFailureCount: Math.max(0, Number(recentOnchainFailureCount) || 0),
    recentWebsocketDisconnectCount: Math.max(0, Number(recentWebsocketDisconnectCount) || 0),
    websocketPublicClientCount: Math.max(0, Number(websocketPublicClientCount) || 0),
    websocketAgentClientCount: Math.max(0, Number(websocketAgentClientCount) || 0),
  };
};

export const evaluateMonitoringAlerts = snapshot => {
  const alerts = [];
  if (!snapshot) return alerts;

  if (
    snapshot.lastFeedEventAgeMs !== null &&
    snapshot.lastFeedEventAgeMs > snapshot.thresholds.feedStaleMs
  ) {
    alerts.push({
      code: "FEED_STALE",
      severity: "critical",
      message: `Market feed has been stale for ${snapshot.lastFeedEventAgeMs}ms.`,
    });
  }

  if (snapshot.reconciliationEnabled) {
    if (snapshot.lastReconciliationSuccessAtMs === null) {
      alerts.push({
        code: "RECONCILIATION_NEVER_SUCCEEDED",
        severity: "critical",
        message: "Chain reconciliation has not completed successfully yet.",
      });
    } else if (snapshot.lastReconciliationSuccessAgeMs > snapshot.thresholds.reconciliationStaleMs) {
      alerts.push({
        code: "RECONCILIATION_STALE",
        severity: "critical",
        message: `Chain reconciliation has been stale for ${snapshot.lastReconciliationSuccessAgeMs}ms.`,
      });
    }
  }

  if (
    snapshot.pendingNonceInvalidations > 0 &&
    snapshot.oldestPendingNonceAgeMs !== null &&
    snapshot.oldestPendingNonceAgeMs > snapshot.thresholds.pendingActionStaleMs
  ) {
    alerts.push({
      code: "PENDING_NONCE_INVALIDATIONS_STUCK",
      severity: "critical",
      message: `${snapshot.pendingNonceInvalidations} nonce invalidations are pending; oldest age ${snapshot.oldestPendingNonceAgeMs}ms.`,
    });
  }

  if (snapshot.failedNonceInvalidations > 0) {
    alerts.push({
      code: "FAILED_NONCE_INVALIDATIONS_PRESENT",
      severity: "warning",
      message: `${snapshot.failedNonceInvalidations} nonce invalidations are in failed_onchain state.`,
    });
  }

  if (
    snapshot.relayWalletBalanceEth !== null &&
    snapshot.relayWalletBalanceEth < snapshot.thresholds.relayWalletMinEth
  ) {
    alerts.push({
      code: "RELAY_WALLET_LOW_BALANCE",
      severity: "critical",
      message: `Relay wallet balance is low at ${snapshot.relayWalletBalanceEth} ETH.`,
    });
  }

  if (snapshot.recentOnchainFailureCount > 0) {
    alerts.push({
      code: "RECENT_ONCHAIN_FAILURES",
      severity: "warning",
      message: `${snapshot.recentOnchainFailureCount} onchain relay actions failed within the lookback window.`,
    });
  }

  if (snapshot.recentWebsocketDisconnectCount > snapshot.thresholds.websocketDisconnectMaxCount) {
    alerts.push({
      code: "WEBSOCKET_DISCONNECT_SPIKE",
      severity: "warning",
      message: `${snapshot.recentWebsocketDisconnectCount} websocket disconnects occurred within the monitoring lookback window.`,
    });
  }

  return alerts;
};
