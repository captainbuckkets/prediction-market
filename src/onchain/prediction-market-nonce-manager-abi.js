export const predictionMarketNonceManagerAbi = [
  {
    type: "event",
    name: "OwnershipTransferred",
    inputs: [
      { name: "previousOwner", type: "address", indexed: true },
      { name: "newOwner", type: "address", indexed: true },
    ],
  },
  {
    type: "event",
    name: "RelayerAuthorizationUpdated",
    inputs: [
      { name: "relayer", type: "address", indexed: true },
      { name: "authorized", type: "bool", indexed: false },
    ],
  },
  {
    type: "event",
    name: "NonceInvalidated",
    inputs: [
      { name: "marketId", type: "bytes32", indexed: true },
      { name: "maker", type: "address", indexed: true },
      { name: "nonce", type: "uint64", indexed: true },
      { name: "relayer", type: "address", indexed: false },
    ],
  },
  {
    type: "function",
    stateMutability: "nonpayable",
    name: "authorizeRelayer",
    inputs: [
      { name: "relayer", type: "address" },
      { name: "authorized", type: "bool" },
    ],
    outputs: [],
  },
  {
    type: "function",
    stateMutability: "nonpayable",
    name: "invalidateNonce",
    inputs: [
      { name: "marketId", type: "bytes32" },
      { name: "maker", type: "address" },
      { name: "nonce", type: "uint64" },
    ],
    outputs: [],
  },
  {
    type: "function",
    stateMutability: "view",
    name: "isNonceInvalidated",
    inputs: [
      { name: "marketId", type: "bytes32" },
      { name: "maker", type: "address" },
      { name: "nonce", type: "uint64" },
    ],
    outputs: [{ name: "", type: "bool" }],
  },
  {
    type: "function",
    stateMutability: "view",
    name: "owner",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
  },
  {
    type: "function",
    stateMutability: "view",
    name: "relayerAuthorized",
    inputs: [{ name: "relayer", type: "address" }],
    outputs: [{ name: "", type: "bool" }],
  },
  {
    type: "function",
    stateMutability: "nonpayable",
    name: "transferOwnership",
    inputs: [{ name: "nextOwner", type: "address" }],
    outputs: [],
  },
];
