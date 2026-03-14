import { createPublicClient, createWalletClient, http, parseEther } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { abstractTestnet } from "viem/chains";
import {
  PredictionMarketsAgentClient,
  issueWalletAuthKey,
  requestWalletAuthChallenge,
} from "../src/agent-client.ts";

const BASE_URL = process.env.PM_BASE_URL || "http://127.0.0.1:3002/prediction-markets";
const RPC_URL = process.env.PM_RPC_URL || "https://api.testnet.abs.xyz";
const MAKER_PRIVATE_KEY = process.env.PM_MAKER_PRIVATE_KEY || "";
const VAULT_DEPOSIT_ETH = process.env.PM_VAULT_DEPOSIT_ETH || "0.001";
const VAULT_ADDRESS_ENV = process.env.PM_VAULT_ADDRESS || "";
const VAULT_ABI = [
  {
    type: "function",
    name: "deposit",
    stateMutability: "payable",
    inputs: [{ name: "amountMicrousd", type: "uint256" }],
    outputs: [],
  },
] as const;

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

  const faucetRequest = await client.requestFaucet();
  logEvent("faucet request", faucetRequest);

  const protocol = await client.getProtocol();
  const vaultAddress =
    VAULT_ADDRESS_ENV ||
    String((protocol as { funding?: { vault_address?: string | null } }).funding?.vault_address || "");
  if (!vaultAddress || !vaultAddress.startsWith("0x")) {
    throw new Error("Vault address is not available. Set PM_VAULT_ADDRESS or use a backend that exposes funding.vault_address.");
  }

  const walletClient = createWalletClient({
    account,
    chain: abstractTestnet,
    transport: http(RPC_URL),
  });
  const publicClient = createPublicClient({
    chain: abstractTestnet,
    transport: http(RPC_URL),
  });

  const depositValue = parseEther(VAULT_DEPOSIT_ETH);
  const txHash = await walletClient.writeContract({
    address: vaultAddress as `0x${string}`,
    abi: VAULT_ABI,
    functionName: "deposit",
    args: [depositValue],
    account,
    chain: abstractTestnet,
    value: depositValue,
  });
  const receipt = await publicClient.waitForTransactionReceipt({ hash: txHash });
  logEvent("vault deposit", {
    vault_address: vaultAddress,
    deposit_eth: VAULT_DEPOSIT_ETH,
    tx_hash: txHash,
    status: receipt.status,
  });

  const me = await client.getMe();
  logEvent("me", me);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
