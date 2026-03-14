import json
import os
import sys
import time

from src.agent_client import PredictionMarketsAgentClient


BASE_URL = os.environ.get("PM_BASE_URL", "http://127.0.0.1:3002/prediction-markets")
API_KEY = os.environ.get("PM_AGENT_API_KEY", "")
PLACE_ORDER = os.environ.get("PM_PLACE_ORDER") == "1"
ORDER_SIDE = os.environ.get("PM_ORDER_SIDE", "buy_yes")
ORDER_PRICE_TENTHS = int(os.environ.get("PM_ORDER_PRICE_TENTHS", "543"))
ORDER_SIZE_SHARES = float(os.environ.get("PM_ORDER_SIZE_SHARES", "1"))

def log_event(label, payload):
    print(f"\n[{label}]")
    print(json.dumps(payload, indent=2))

def bootstrap_rest(client):
    status = client.get_status()
    current_market = client.get_current_market()
    orders = client.get_orders()
    fills = client.get_fills()
    positions = client.get_positions()
    claims = client.get_claims()
    log_event("status", status)
    log_event("current market", current_market)
    log_event("orders", orders)
    log_event("fills", fills)
    log_event("positions", positions)
    log_event("claims", claims)
    return current_market


def main():
    if not API_KEY:
        print("PM_AGENT_API_KEY is required.", file=sys.stderr)
        sys.exit(1)

    client = PredictionMarketsAgentClient(BASE_URL, API_KEY)
    current_market = bootstrap_rest(client)
    ws = client.connect()
    print("\n[agent ws] connected")

    try:
        while True:
            message = client.recv()
            event_type = message.get("type", "unknown")
            payload = message.get("payload", {})
            log_event(event_type, payload)

            if event_type == "agent.bootstrap":
                market_id = payload.get("market_id") or current_market.get("market_id")
                client.subscribe_book(market_id=market_id)
                client.subscribe_positions(market_id=market_id)
                client.subscribe_nonce_invalidations(market_id=market_id)

                if PLACE_ORDER and market_id and payload.get("current_round", {}).get("endMs"):
                    expiry_ms = int(payload["current_round"]["endMs"]) + 30000
                    client.place_order(
                        market_id=market_id,
                        side=ORDER_SIDE,
                        price_tenths=ORDER_PRICE_TENTHS,
                        size_shares=ORDER_SIZE_SHARES,
                        expiry_ms=expiry_ms,
                        client_order_id=f"py-demo-{int(time.time() * 1000)}",
                    )
            elif event_type == "stream.recovery_required":
                print("Recovery required. Resubscribe without since_sequence or rebuild local state from the snapshot.")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
