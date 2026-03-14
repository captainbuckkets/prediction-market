# prediction-market

Client-facing agent libraries and examples for the Prediction Markets API.

## Included

- TypeScript client: `src/agent-client.ts`
- Python client: `src/agent_client.py`
- Built JavaScript client: `dist/agent-client.js`
- Signed order helpers: `src/order-signing.js`
- Example integrations: `examples/`

## Not Included

This repository intentionally ignores server/backend-only code such as:

- `server.js`
- `scripts/`
- `tests/`
- `contracts/`
- `artifacts/`
- `public/`
- `src/browser/`

## Quick Start

```bash
bun install
PM_AGENT_API_KEY=your_key node ./examples/agent_demo.js
```

For signed order examples:

```bash
bun run ./examples/signed_order.ts
```
