import { privateKeyToAccount } from "viem/accounts";
import {
  PredictionMarketsAgentClient,
  issueWalletAuthKey,
  requestWalletAuthChallenge,
} from "../src/agent-client.ts";

const BASE_URL = process.env.PM_BASE_URL || "http://127.0.0.1:3002/prediction-markets";
const MAKER_PRIVATE_KEY = process.env.PM_MAKER_PRIVATE_KEY || "";

if (!MAKER_PRIVATE_KEY) {
  console.error("PM_MAKER_PRIVATE_KEY is required.");
  process.exit(1);
}

function logEvent(label: string, payload: unknown) {
  console.log(`\n[${label}]`);
  console.log(JSON.stringify(payload, null, 2));
}

async function main() {
  const account = privateKeyToAccount(MAKER_PRIVATE_KEY as `0x${string}`);

  const challenge = await requestWalletAuthChallenge(BASE_URL, account.address);
  logEvent("challenge", challenge);

  const signature = await account.signMessage({ message: challenge.message });
  const issued = await issueWalletAuthKey(BASE_URL, {
    challenge_id: challenge.challenge_id,
    wallet_address: account.address,
    signature,
  });
  logEvent("issued key", {
    ...issued,
    api_key: `${issued.api_key.slice(0, 12)}...`,
  });

  const client = new PredictionMarketsAgentClient({
    baseUrl: BASE_URL,
    apiKey: issued.api_key,
  });
  const me = await client.getMe();
  logEvent("me", me);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
