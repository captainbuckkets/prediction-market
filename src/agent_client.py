import json
import urllib.parse
import urllib.error
import urllib.request

try:
    import websocket
except ImportError as exc:  # pragma: no cover - import error path depends on environment
    raise RuntimeError("Install websocket-client first: pip install websocket-client") from exc


class PredictionMarketsAgentClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.ws = None

    def auth_headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def request_json(self, path, method="GET", body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            method=method,
            headers=self.auth_headers(),
            data=data,
        )
        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            payload = error.read().decode("utf-8")
            raise RuntimeError(f"{path} failed with {error.code}: {payload}") from error

    def get_status(self):
        return self.request_json("/api/status")

    def get_protocol(self):
        return self.request_json("/api/agent/protocol")

    def get_schemas(self):
        return self.request_json("/api/agent/schemas")

    def get_me(self):
        return self.request_json("/api/me")

    def get_account_summary(self):
        return self.request_json("/api/account/summary")

    def get_markets(self):
        return self.request_json("/api/markets")

    def get_notifications(self):
        return self.request_json("/api/notifications")

    def create_notification(self, payload):
        return self.request_json("/api/notifications", method="POST", body=payload)

    def update_notification(self, notification_id, payload):
        return self.request_json(f"/api/notifications/{urllib.parse.quote(str(notification_id), safe='')}", method="PATCH", body=payload)

    def delete_notification(self, notification_id):
        return self.request_json(f"/api/notifications/{urllib.parse.quote(str(notification_id), safe='')}", method="DELETE")

    def get_current_market(self):
        return self.request_json("/api/markets/current")

    def get_orders(self):
        return self.request_json("/api/orders")

    def get_order(self, order_hash):
        return self.request_json(f"/api/orders/{urllib.parse.quote(str(order_hash), safe='')}")

    def get_order_events(self):
        return self.request_json("/api/order-events")

    def get_fills(self):
        return self.request_json("/api/fills")

    def get_positions(self):
        return self.request_json("/api/positions")

    def get_claims(self):
        return self.request_json("/api/claims")

    def get_nonce_invalidations(self):
        return self.request_json("/api/nonce-invalidations")

    def submit_claim(self, market_id):
        return self.request_json("/api/claims", method="POST", body={"market_id": market_id})

    def place_order_rest(self, payload):
        return self.request_json("/api/orders/place", method="POST", body=payload)

    def cancel_order_rest(self, order_hash):
        return self.request_json("/api/orders/cancel", method="POST", body={"order_hash": order_hash})

    def take_order_rest(self, order_hash, size_shares, taker_address=None):
        payload = {
            "order_hash": order_hash,
            "size_shares": size_shares,
        }
        if taker_address is not None:
            payload["taker_address"] = taker_address
        return self.request_json("/api/orders/take", method="POST", body=payload)

    def build_agent_ws_url(self):
        if self.base_url.startswith("https://"):
            ws_base = "wss://" + self.base_url[len("https://") :]
        else:
            ws_base = "ws://" + self.base_url[len("http://") :]
        return f"{ws_base}/ws/agent?api_key={self.api_key}"

    def connect(self):
        self.ws = websocket.create_connection(self.build_agent_ws_url())
        return self.ws

    def disconnect(self):
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass
        self.ws = None

    def send(self, event_type, payload=None):
        if self.ws is None:
            raise RuntimeError("Agent websocket is not connected.")
        self.ws.send(json.dumps({"type": event_type, "payload": payload or {}}))

    def recv(self):
        if self.ws is None:
            raise RuntimeError("Agent websocket is not connected.")
        raw_message = self.ws.recv()
        return json.loads(raw_message)

    def subscribe_book(self, market_id=None, since_sequence=None):
        self.send(
            "book.subscribe",
            {
                "market_id": market_id,
                "since_sequence": since_sequence,
            },
        )

    def subscribe_positions(self, market_id=None, since_sequence=None):
        self.send(
            "positions.subscribe",
            {
                "market_id": market_id,
                "since_sequence": since_sequence,
            },
        )

    def subscribe_nonce_invalidations(self, market_id=None, since_sequence=None):
        self.send(
            "nonce_invalidations.subscribe",
            {
                "market_id": market_id,
                "since_sequence": since_sequence,
            },
        )

    def place_order(
        self,
        market_id,
        side,
        price_tenths,
        size_shares,
        expiry_ms=None,
        client_order_id=None,
        maker_address=None,
        nonce=None,
        signature=None,
    ):
        payload = {
            "market_id": market_id,
            "side": side,
            "price_tenths": price_tenths,
            "size_shares": size_shares,
        }
        if expiry_ms is not None:
            payload["expiry_ms"] = expiry_ms
        if client_order_id is not None:
            payload["client_order_id"] = client_order_id
        if maker_address is not None:
            payload["maker_address"] = maker_address
        if nonce is not None:
            payload["nonce"] = nonce
        if signature is not None:
            payload["signature"] = signature
        self.send("order.place", payload)

    def cancel_order(self, order_hash):
        self.send("order.cancel", {"order_hash": order_hash})

    def take_order(self, order_hash, size_shares, taker_address=None):
        payload = {
            "order_hash": order_hash,
            "size_shares": size_shares,
        }
        if taker_address is not None:
            payload["taker_address"] = taker_address
        self.send("order.take", payload)
