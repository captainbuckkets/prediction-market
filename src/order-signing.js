import { getAddress, hashTypedData, recoverTypedDataAddress, zeroAddress } from "viem";

export const ORDERBOOK_DOMAIN_NAME = "PredictionMarketsOrderbook";
export const ORDERBOOK_DOMAIN_VERSION = "1";
export const ORDERBOOK_DEFAULT_CHAIN_ID = 11124;
export const ORDERBOOK_DEFAULT_VERIFYING_CONTRACT = zeroAddress;
export const ORDERBOOK_PRIMARY_TYPE = "MakerOrder";
export const ORDERBOOK_TYPES = {
  [ORDERBOOK_PRIMARY_TYPE]: [
    { name: "marketId", type: "string" },
    { name: "maker", type: "address" },
    { name: "side", type: "string" },
    { name: "priceTenths", type: "uint16" },
    { name: "sizeMicroshares", type: "uint256" },
    { name: "nonce", type: "uint64" },
    { name: "expiryMs", type: "uint64" },
  ],
};

export const sharesToMicroshares = value => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    throw new Error("size_shares must be positive.");
  }
  return BigInt(Math.round(numeric * 1_000_000));
};

export const microsharesToShares = value => Number(value) / 1_000_000;

export const normalizePredictionMarketOrderMessage = ({
  market_id,
  maker_address,
  side,
  price_tenths,
  size_shares,
  nonce,
  expiry_ms,
}) => ({
  marketId: String(market_id),
  maker: getAddress(maker_address),
  side: String(side),
  priceTenths: Number(price_tenths),
  sizeMicroshares: sharesToMicroshares(size_shares),
  nonce: BigInt(Number(nonce)),
  expiryMs: BigInt(Number(expiry_ms)),
});

export const buildPredictionMarketOrderTypedData = ({
  message,
  chainId = ORDERBOOK_DEFAULT_CHAIN_ID,
  verifyingContract = ORDERBOOK_DEFAULT_VERIFYING_CONTRACT,
}) => ({
  domain: {
    name: ORDERBOOK_DOMAIN_NAME,
    version: ORDERBOOK_DOMAIN_VERSION,
    chainId,
    verifyingContract,
  },
  primaryType: ORDERBOOK_PRIMARY_TYPE,
  types: ORDERBOOK_TYPES,
  message,
});

export const hashPredictionMarketOrder = parameters => hashTypedData(buildPredictionMarketOrderTypedData(parameters));

export const recoverPredictionMarketOrderSigner = async ({ signature, ...parameters }) =>
  recoverTypedDataAddress({
    ...buildPredictionMarketOrderTypedData(parameters),
    signature,
  });
