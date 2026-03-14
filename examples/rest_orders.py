import json
import os
import sys

from src.agent_client import PredictionMarketsAgentClient


BASE_URL = os.environ.get("PM_BASE_URL", "http://127.0.0.1:3002/prediction-markets")
API_KEY = os.environ.get("PM_AGENT_API_KEY", "")
ORDER_SIDE = os.environ.get("PM_ORDER_SIDE", "buy_yes")
ORDER_PRICE_TENTHS = int(os.environ.get("PM_ORDER_PRICE_TENTHS", "543"))
ORDER_SIZE_SHARES = float(os.environ.get("PM_ORDER_SIZE_SHARES", "1"))
CANCEL_AFTER_PLACE = os.environ.get("PM_CANCEL_AFTER_PLACE", "1") != "0"


def log_event(label, payload):
    print(f"\n[{label}]")
    print(json.dumps(payload, indent=2))


def main():
    if not API_KEY:
        print("PM_AGENT_API_KEY is required.", file=sys.stderr)
        sys.exit(1)

    client = PredictionMarketsAgentClient(BASE_URL, API_KEY)
    me = client.get_me()
    summary = client.get_account_summary()
    current_market = client.get_current_market()
    log_event("me", me)
    log_event("account summary", summary)
    log_event("current market", current_market)

    market_id = current_market.get("market_id")
    if not market_id:
        raise RuntimeError("No active market.")

    placed = client.place_order_rest(
        {
            "market_id": market_id,
            "side": ORDER_SIDE,
            "price_tenths": ORDER_PRICE_TENTHS,
            "size_shares": ORDER_SIZE_SHARES,
            "client_order_id": f"rest-py-{int(__import__('time').time() * 1000)}",
        }
    )
    log_event("placed", placed)

    fetched = client.get_order(placed["order"]["order_hash"])
    log_event("fetched", fetched)

    if CANCEL_AFTER_PLACE:
      cancelled = client.cancel_order_rest(placed["order"]["order_hash"])
      log_event("cancelled", cancelled)


if __name__ == "__main__":
    main()
