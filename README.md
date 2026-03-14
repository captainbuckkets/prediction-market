# prediction-market

Client-facing agent libraries and examples for the Prediction Markets API.

This repository contains only client libraries, signing helpers, and example integrations.
It does not include the Prediction Markets backend/server implementation.

## Included

- TypeScript client: `src/agent-client.ts`
- Python client: `src/agent_client.py`
- Built JavaScript client: `dist/agent-client.js`
- Signed order helpers: `src/order-signing.js`
- Example integrations: `examples/`

## Wallet Key Flow

Bots can self-issue a normal non-admin API key by proving control of an EVM wallet:

1. `POST /api/auth/challenge`
2. Sign the returned message with the wallet
3. `POST /api/auth/issue-key`
4. Use the returned `api_key` for REST and websocket auth

The TypeScript client exposes helpers for this flow, and `examples/wallet_issue_key.ts` shows the full sequence.

## Not Included

This repository intentionally ignores server/backend-only code such as:

- `server.js`
- `scripts/`
- `tests/`
- `contracts/`
- `artifacts/`
- `public/`
- `src/browser/`

## Security Notes

- Do not commit `PM_AGENT_API_KEY` or `PM_MAKER_PRIVATE_KEY`.
- Pass credentials through environment variables only.
- The current websocket flow uses `api_key` in the websocket query string for compatibility, so avoid running through proxies or logs you do not control.
- Use disposable or tightly scoped test credentials for public testing.

## Quick Start

```bash
bun install
PM_AGENT_API_KEY=your_key node ./examples/agent_demo.js
```

For signed order examples:

```bash
PM_AGENT_API_KEY=your_key \
PM_MAKER_PRIVATE_KEY=0xyourprivatekey \
bun run ./examples/signed_order.ts
```

For wallet-based key issuance:

```bash
PM_MAKER_PRIVATE_KEY=0xyourprivatekey \
bun run ./examples/wallet_issue_key.ts
```
