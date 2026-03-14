# Agent API Examples

These examples talk directly to the prediction markets agent API and websocket.

Files:
- `wallet_issue_key.ts`: TypeScript example that requests a wallet auth challenge, signs it, issues a normal API key, and verifies the key on `/api/me`
- `agent_demo.js`: JavaScript demo for Bun or modern Node.js with global `fetch` and `WebSocket`
- `agent_demo.py`: Python demo using the reusable Python client
- `agent_demo.ts`: typed TypeScript version of the JavaScript flow
- `rest_orders.js`: JavaScript REST place/fetch/cancel flow
- `rest_orders.py`: Python REST place/fetch/cancel flow
- `ws_rest_hybrid.ts`: TypeScript example that subscribes on websocket and mutates via REST
- `signed_order.ts`: TypeScript example that signs and places a maker order using the live protocol signing config
- `agent_demo.go`: small Go scaffold using direct REST plus the agent websocket
- `../src/agent-client.ts`: reusable TypeScript client with typed REST methods, websocket helpers, reconnect support, and replay-aware subscribe helpers
- `../src/agent_client.py`: reusable Python client with REST helpers and websocket trading/subscription helpers
- `../dist/agent-client.js`: built JavaScript entrypoint for plain JS consumers

Environment:
- `PM_BASE_URL`
  - default: `http://127.0.0.1:3002/prediction-markets`
- `PM_AGENT_API_KEY`
  - required
- `PM_MAKER_PRIVATE_KEY`
  - required for wallet key issuance and signed order examples
- `PM_PLACE_ORDER`
  - optional: set to `1` to place a sample order after bootstrap

Optional order settings:
- `PM_ORDER_SIDE`
  - default: `buy_yes`
- `PM_ORDER_PRICE_TENTHS`
  - default: `543`
- `PM_ORDER_SIZE_SHARES`
  - default: `1`

JavaScript:

```sh
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key node ./examples/agent_demo.js
```

JavaScript REST place/cancel:

```sh
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key node ./examples/rest_orders.js
```

Python:

```sh
pip install websocket-client
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key python3 ./examples/agent_demo.py
```

Python REST place/cancel:

```sh
pip install websocket-client
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key python3 ./examples/rest_orders.py
```

TypeScript:

```sh
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key bun run ./examples/agent_demo.ts
```

Wallet key issuance:

```sh
cd /root/prediction-markets
PM_MAKER_PRIVATE_KEY=0x... bun run ./examples/wallet_issue_key.ts
```

Hybrid websocket observe + REST mutate:

```sh
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key bun run ./examples/ws_rest_hybrid.ts
```

Signed order:

```sh
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key \
PM_MAKER_PRIVATE_KEY=0x... \
bun run ./examples/signed_order.ts
```

Reusable TypeScript client:

```ts
import {
  PredictionMarketsAgentClient,
  issueWalletAuthKey,
  requestWalletAuthChallenge,
} from "../src/agent-client.ts";

const baseUrl = "http://127.0.0.1:3002/prediction-markets";
const challenge = await requestWalletAuthChallenge(baseUrl, wallet.address);
const signature = await wallet.signMessage({ message: challenge.message });
const issued = await issueWalletAuthKey(baseUrl, {
  challenge_id: challenge.challenge_id,
  wallet_address: wallet.address,
  signature,
});

const client = new PredictionMarketsAgentClient({
  baseUrl,
  apiKey: issued.api_key,
  reconnect: true,
});
const orders = await client.getOrders();
client.onMessage = message => {
  console.log(message.type, message.payload);
};
client.connect();
```

Reusable JavaScript client:

```js
import { PredictionMarketsAgentClient } from "prediction-markets/agent-client";

const client = new PredictionMarketsAgentClient({
  baseUrl: "http://127.0.0.1:3002/prediction-markets",
  apiKey: process.env.PM_AGENT_API_KEY,
  reconnect: true,
});

const orders = await client.getOrders();
client.connect();
```

Reusable Python client:

```py
from src.agent_client import PredictionMarketsAgentClient

client = PredictionMarketsAgentClient(
    base_url="http://127.0.0.1:3002/prediction-markets",
    api_key=os.environ["PM_AGENT_API_KEY"],
)

orders = client.get_orders()
client.connect()
client.subscribe_book()
message = client.recv()
print(message)
```

Go:

```sh
go get github.com/coder/websocket
cd /root/prediction-markets
PM_AGENT_API_KEY=local-dev-agent-key go run ./examples/agent_demo.go
```

Suggested additional languages:
- TypeScript: best if you want stronger message typing on top of the JavaScript example
- Go: good for long-running agents and websocket reconnect/replay handling
- Bash/curl: useful for admin key-management and quick smoke tests, but not ideal for websocket agents
