# IBC v2 Relayer

![IBC v2 Relayer](.github/assets/cosmos-ibc-relayer-github.svg)

IBC v2 Relayer is a relaying service for the IBC v2 Protocol. The relayer supports interoperating between a Cosmos-based chain and major EVM networks.

> **Note:** This repository is a public mirror of an internal codebase. Commits here are synced from upstream; pull requests may be applied internally rather than merged directly.

## Relaying Sequence
![Relaying Sequence](./relaying-sequence.png)

## Supported Features

- Compatible with all major EVM chains (Ethereum, Base, Optimism, Arbitrum, Polygon, and more)
- Request-driven design for configurable, on-demand relaying
- Transaction failure retry support
  - Re-orgs
  - Out-of-gas
  - Inadequate gas price
  - Tx network propagation fails to reach leader
  - Invalid by the network, but valid by the submitting node
- Transaction Tracking API
- Remote signing support
- Configurable packet delivery latency via batching
- Ability to blacklist addresses (ex: OFAC)
- Transaction cost tracking

## Getting Started

### Prerequisites

- Go 1.24+
- Docker and Docker Compose
- A running [proof API](https://github.com/cosmos/solidity-ibc-eureka/tree/main/programs/proof-api) service
- RPC endpoints for the chains you want to relay between

### Local Development

1. Start Postgres:
```bash
docker compose up -d --wait
```

2. Apply database migrations (see [Database Migrations](#database-migrations) for details):
```bash
make build
./bin/relayer migrate --config ./config/local/config.yml
```

3. Create a local config file (see [Configuration Reference](#configuration-reference) below).

4. Create a local keys file (see [Local Signing](#local-signing) below).

5. Run the relayer:
```bash
make relayer-local
```

The relayer will start:
- gRPC API server on the address configured in `relayer_api.address`
- Prometheus metrics server on the configured address
- Relay dispatcher polling for new transfers

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `./config/local/config.yml` | Path to relayer config file |
| `--ibcv2-relaying` | `true` | Enable/disable the relay dispatcher |
| `--db-migrate` | `false` | Apply pending database migrations before starting the relayer |

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `migrate` | Apply pending database migrations and exit (does not start the relayer) |

### Running Tests

```bash
make test
```

## Database Migrations

Migrations are compiled into the relayer binary via Go's `embed` directive — the same image that runs the relayer also applies the schema. There are two invocation modes:

**One-shot (recommended for production deployments):** apply pending migrations and exit. Run this from a Kubernetes initContainer, CI step, or pre-deploy hook.

```bash
./bin/relayer migrate --config /path/to/config.yml
```

**Combined startup (convenient for local development):** apply pending migrations, then continue into normal relayer startup.

```bash
./bin/relayer --db-migrate --config /path/to/config.yml
```

Each migration runs in its own transaction; on failure the relayer aborts startup rather than serve against an unverified schema.

`make test` exercises this path automatically — it builds the relayer, brings up Postgres, and runs `./bin/relayer migrate` before invoking `go test`.

### Migration Files

Migration files live in [`./db/migrations/`](./db/migrations/). They are embedded at build time via [`db/migrate.go`](./db/migrate.go), so no separate volume mount or sidecar image is needed.

### Breaking change in this release

The standalone `ibc-relayer-migrate` Docker image is no longer published. Deployments that previously pulled `ghcr.io/cosmos/ibc-relayer-migrate:<version>` must switch to invoking `migrate` on the main `ibc-relayer` image:

```bash
# before
docker run --rm ghcr.io/cosmos/ibc-relayer-migrate:<version> up

# after
docker run --rm ghcr.io/cosmos/ibc-relayer:<version> migrate --config /path/to/config.yml
```

## Design
![Design](./relayer-design.png)

The relayer has three main components - the gRPC server which clients use to interact with the relayer, a Postgres db, and the core relayer. The gRPC server populates the db with packets, which the core relayer monitors and updates as it progresses in relaying those packets.

## API Interface
The relayer serves a gRPC server which clients use to specify what packets to relay and track packet relaying progress.

```proto
service RelayerApiService {
    // Relay is used to specify a source tx hash for packets the relayer should relay.
    // The relayer will identify all packets created by the transaction and attempt to relay them all.
    rpc Relay(RelayRequest) returns (RelayResponse) {}

    // The status endpoint is used to track the progress of packet relaying.
    // It takes a transaction hash and returns the status of any relevant packets the relayer is aware of.
    // The transaction must first have been passed to the relay endpoint.
    rpc Status(StatusRequest) returns (StatusResponse) {}
}

message StatusRequest {
    string tx_hash = 1;
    string chain_id = 2;
}

enum TransferState {
    TRANSFER_STATE_UNKNOWN = 0;
    TRANSFER_STATE_PENDING = 1;
    TRANSFER_STATE_COMPLETE = 2;
    TRANSFER_STATE_FAILED = 3;
}

message TransactionInfo {
    string tx_hash = 1;
    string chain_id = 2;
}

message PacketStatus {
    TransferState state = 1;
    uint64 sequence_number = 2;
    string source_client_id = 3;
    TransactionInfo send_tx = 4;
    TransactionInfo recv_tx = 5;
    TransactionInfo ack_tx = 6;
    TransactionInfo timeout_tx = 7;
}

message StatusResponse {
    repeated PacketStatus packet_statuses = 1;
}

message RelayRequest {
    string tx_hash = 1;
    string chain_id = 2;
}

message RelayResponse {}
```

## Observability

Alert setup guidance for customers lives in [`./docs/alerts.md`](./docs/alerts.md).

| Type      | Name                                   | Description |
|-----------|----------------------------------------|-------------|
| Metric    | Relayer api request count               | Paginated by method and response code |
| Metric    | Relayer api request latency             | Paginated by method |
| Metric    | Transfer count                          | Paginated by source, destination chain, and transfer state |
| Metric    | Relayer gas balance                     | Paginated by chain and gas token |
| Metric    | Relayer gas balance state               | A gauge where each value represents a gas balance state. 0 = ok, 1 = warning, 2 = critical. The thresholds that define each state are defined in the relayer configuration. Paginated by chain |
| Metric    | External request count                  | Paginated by endpoint, method and response code |
| Metric    | External request latency                | Paginated by endpoint and method |
| Metric    | Transactions submitted counter          | Paginated by node response success status and chain |
| Metric    | Transaction retry counter               | Paginated by source and destination chain |
| Metric    | Transactions confirmed counter          | Paginated by execution success and chain |
| Metric    | Transaction gas cost counter            | Paginated by chain |
| Metric    | Relay latency                           | Time between send tx and ack/timeout tx. Paginated by source and destination chain |
| Metric    | Detected client update required counter | Paginated by chain |
| Metric    | Client updated counter                  | Paginated by chain |
| Metric    | Excessive relay latency counter         | Incremented anytime a transfer is pending for longer than some configured threshold. Paginated by source and destination chain |
| Alert     | Excessive relay latency                 | Should alert whenever the excessive relay latency counter increases |
| Alert     | Excessive gas usage                     | Should alert whenever the gas cost counter increases faster than some threshold |
| Alert     | Low gas balance                         | Should alert whenever the relayer gas balance state metric is in the warning or critical state |

## Configuration Reference

The relayer is configured via a YAML file. The example below is a representative starting point, not an exhaustive schema reference.

`ibcv2_proof_api.proof_cache_ttl` (default `30m`) and `ibcv2_proof_api.proof_cache_max_uses` (default `5`) bound how long, and for how many delivery attempts, a proof already bought for a set of packets is reused when delivering it fails, so a retry does not buy the proof again.

### Full Example

```yaml
postgres:
  hostname: localhost
  port: "42500"
  database: relayer

metrics:
  prometheus_address: "0.0.0.0:8888"

relayer_api:
  address: "0.0.0.0:9000"

ibcv2_proof_api:
  grpc_address: "localhost:50051"
  grpc_tls_enabled: false

signing:
  # Local signing — set keys_path to use local key file
  keys_path: "./config/local/ibcv2keys.json"
  # Remote signing — set grpc_address to use remote signer (takes precedence over keys_path)
  # grpc_address: "localhost:50052"
  # cosmos_wallet_key: "cosmos-wallet-id"
  # evm_wallet_key: "evm-wallet-id"
  # svm_wallet_key: "svm-wallet-id"

coingecko:
  base_url: "https://pro-api.coingecko.com/api/v3"
  api_key: "your-api-key"
  requests_per_minute: 30
  cache_refresh_interval: 5m

chains:
  cosmoshub:
    chain_name: "cosmoshub"
    chain_id: "cosmoshub-4"
    type: "cosmos"
    environment: "mainnet"
    gas_token_symbol: "ATOM"
    gas_token_coingecko_id: "cosmos"
    gas_token_decimals: 6
    supported_bridges:
      - ibcv2
    cosmos:
      ibcv2_tx_fee_denom: "uatom"
      ibcv2_tx_fee_amount: 5000
      rpc: "https://cosmos-rpc.example.com"
      rpc_basic_auth_var: "COSMOS_RPC_AUTH"
      grpc: "cosmos-grpc.example.com:9090"
      grpc_tls_enabled: true
      address_prefix: "cosmos"
    ibcv2:
      counterparty_chains:
        "08-wasm-0": "1"   # client ID on cosmoshub → ethereum chain ID
      finality_offset: 10
      recv_batch_size: 50
      recv_batch_timeout: 10s
      recv_batch_concurrency: 3
      ack_batch_size: 50
      ack_batch_timeout: 10s
      ack_batch_concurrency: 3
      timeout_batch_size: 50
      timeout_batch_timeout: 10s
      timeout_batch_concurrency: 3
      should_relay_success_acks: true
      should_relay_error_acks: true
    signer_gas_alert_thresholds:
      ibcv2:
        warning_threshold: "5000000"    # in smallest denom units (uatom)
        critical_threshold: "1000000"

  ethereum:
    chain_name: "ethereum"
    chain_id: "1"
    type: "evm"
    environment: "mainnet"
    gas_token_symbol: "ETH"
    gas_token_coingecko_id: "ethereum"
    gas_token_decimals: 18
    supported_bridges:
      - ibcv2
    evm:
      rpc: "https://eth-mainnet.g.alchemy.com/v2/your-key"
      rpc_basic_auth_var: "ETH_RPC_AUTH"
      contracts:
        ics_26_router_address: "0x..."
        ics_20_transfer_address: "0x..."
      gas_fee_cap_multiplier: 1.5
      gas_tip_cap_multiplier: 1.2
    ibcv2:
      counterparty_chains:
        "tendermint-0": "cosmoshub-4"   # client ID on ethereum → cosmoshub chain ID
      recv_batch_size: 100
      recv_batch_timeout: 10s
      recv_batch_concurrency: 3
      ack_batch_size: 100
      ack_batch_timeout: 10s
      ack_batch_concurrency: 3
      timeout_batch_size: 100
      timeout_batch_timeout: 10s
      timeout_batch_concurrency: 3
      should_relay_success_acks: true
      should_relay_error_acks: true
    signer_gas_alert_thresholds:
      ibcv2:
        warning_threshold: "1000000000000000000"   # 1 ETH
        critical_threshold: "500000000000000000"   # 0.5 ETH
```

### Section Reference

#### `postgres`

| Field | Type | Description |
|-------|------|-------------|
| `hostname` | string | Postgres host |
| `port` | string | Postgres port |
| `database` | string | Database name |

Database credentials are read from environment variables `POSTGRES_USER` and `POSTGRES_PASSWORD` (default: `relayer`/`relayer`).

#### `metrics`

| Field | Type | Description |
|-------|------|-------------|
| `prometheus_address` | string | Address to serve Prometheus metrics (e.g. `0.0.0.0:8888`) |

#### `relayer_api`

| Field | Type | Description |
|-------|------|-------------|
| `address` | string | Address for the gRPC API server to listen on (e.g. `0.0.0.0:9000`) |

#### `ibcv2_proof_api`

Connection to the proof api service that generates relay transactions.

| Field | Type | Description |
|-------|------|-------------|
| `grpc_address` | string | gRPC address of the proof API |
| `grpc_tls_enabled` | bool | Enable TLS for the proof API connection |
| `timeout` | duration | Deadline for each proof API call, e.g. `20m`. Optional; calls are unbounded when unset. Set it when a proof API proves on demand (the zk corridor's `stellar-proof-api` takes minutes per proof), so a stuck proof frees its batch slot instead of holding it forever. |

#### `signing`

Signing configuration. The mode is inferred from which fields are set:
- If `grpc_address` is set → remote signing (ignores `keys_path`)
- Else if `keys_path` is set → local signing from key file
- Else → fatal error at startup

| Field | Type | Description |
|-------|------|-------------|
| `keys_path` | string | Path to local signing keys JSON file |
| `grpc_address` | string | gRPC address of the remote signer service. If set, takes precedence over `keys_path` |
| `grpc_tls_enabled` | bool | Enable TLS for the remote signer connection. |
| `cosmos_wallet_key` | string | Wallet ID for Cosmos chain signing (remote signer only) |
| `evm_wallet_key` | string | Wallet ID for EVM chain signing (remote signer only) |
| `svm_wallet_key` | string | Wallet ID for Solana chain signing (remote signer only) |

#### `coingecko` (optional)

Used for tracking transaction gas costs in USD. If omitted, gas cost tracking is disabled.

| Field | Type | Description |
|-------|------|-------------|
| `base_url` | string | Coingecko API base URL. May be overridden by `COINGECKO_FULL_URL` |
| `api_key` | string | API key. May be overridden by `COINGECKO_API_KEY`. Optional if the env var is set |
| `requests_per_minute` | int | Rate limit |
| `cache_refresh_interval` | duration | How often to refresh cached prices |

The following environment variables override the corresponding config fields when set and non-empty:

| Env var | Overrides |
|---------|-----------|
| `COINGECKO_FULL_URL` | `base_url` |
| `COINGECKO_API_KEY` | `api_key` |

#### `chains.<chain_key>`

Each entry under `chains` defines a chain the relayer can interact with.

| Field | Type | Description                                           |
|-------|------|-------------------------------------------------------|
| `chain_name` | string | Human-readable chain name. Used primarily in metrics. |
| `chain_id` | string | Chain identifier (numeric for EVM, string for Cosmos) |
| `type` | string | `cosmos`, `evm`, or `svm`                             |
| `environment` | string | `mainnet` or `testnet`                                |
| `gas_token_symbol` | string | Gas token ticker symbol                               |
| `gas_token_coingecko_id` | string | Coingecko ID for gas cost tracking (optional)         |
| `gas_token_decimals` | uint8 | Decimal places for the gas token                      |
| `supported_bridges` | []string | List of bridge types (currently only `ibcv2`)        |

#### `chains.<chain_key>.cosmos`

Required when `type: cosmos`.

| Field | Type | Description |
|-------|------|-------------|
| `gas_price` | float64 | Gas price for fee estimation. Mutually exclusive with `ibcv2_tx_fee_amount` |
| `ibcv2_tx_fee_denom` | string | Fee denom for ibcv2 txs (required if `ibcv2_tx_fee_amount` is set) |
| `ibcv2_tx_fee_amount` | uint64 | Fixed fee amount for ibcv2 txs. Mutually exclusive with `gas_price` |
| `rpc` | string | Tendermint RPC endpoint |
| `rpc_basic_auth_var` | string | Environment variable name containing basic auth credentials for RPC |
| `grpc` | string | gRPC endpoint |
| `grpc_tls_enabled` | bool | Enable TLS for gRPC |
| `address_prefix` | string | Bech32 address prefix (e.g. `cosmos`, `osmo`) |

#### `chains.<chain_key>.evm`

Required when `type: evm`.

| Field | Type | Description |
|-------|------|-------------|
| `rpc` | string | Ethereum JSON-RPC endpoint |
| `rpc_basic_auth_var` | string | Environment variable name containing basic auth credentials for RPC |
| `contracts.ics_26_router_address` | string | ICS26 Router contract address |
| `contracts.ics_20_transfer_address` | string | ICS20 Transfer contract address |
| `gas_fee_cap_multiplier` | float64 | Multiplier applied to the estimated gas fee cap. Optional; defaults to `1.0` when unset. |
| `gas_tip_cap_multiplier` | float64 | Multiplier applied to the estimated gas tip cap. Optional; defaults to `1.0` when unset. |
| `log_lookback_blocks` | uint64 | How many blocks back from the head the router log searches that find an already delivered receive, acknowledgement or timeout start. Optional; searches start at block 0 when unset, which public RPCs such as Sepolia's refuse or time out on. |

`rpc` may be overridden per-chain via an environment variable named `<CHAIN_KEY>_EVM_RPC_FULL_URL`, where `<CHAIN_KEY>` is the upper-cased chain map key with hyphens replaced by underscores. If set and non-empty, it takes precedence over the `rpc` field. Examples:

| Chain key | Env var |
|-----------|---------|
| `ethereum` | `ETHEREUM_EVM_RPC_FULL_URL` |
| `ethereum-testnet` | `ETHEREUM_TESTNET_EVM_RPC_FULL_URL` |
| `base-mainnet` | `BASE_MAINNET_EVM_RPC_FULL_URL` |
| `base-testnet` | `BASE_TESTNET_EVM_RPC_FULL_URL` |

#### `chains.<chain_key>.ibcv2`

IBC v2 relay configuration for this chain. Defines which counterparty chains to relay for and batching behavior.

| Field | Type | Description |
|-------|------|-------------|
| `counterparty_chains` | map[string]string | Maps client IDs on this chain to their counterparty chain IDs. Only connections listed here will be relayed |
| `finality_offset` | uint64 | Number of blocks to wait after a tx before considering it finalized. If omitted, uses the chain's native finality (e.g. `finalized` block tag for EVM) |
| `recv_batch_size` | int | Max packets to accumulate before flushing a recv batch |
| `recv_batch_timeout` | duration | Max time to wait for recv packets to accumulate before flushing |
| `recv_batch_concurrency` | int | Max concurrent recv batches being processed |
| `ack_batch_size` | int | Max packets to accumulate before flushing an ack batch |
| `ack_batch_timeout` | duration | Max time to wait for ack packets to accumulate before flushing |
| `ack_batch_concurrency` | int | Max concurrent ack batches being processed |
| `timeout_batch_size` | int | Max packets to accumulate before flushing a timeout batch |
| `timeout_batch_timeout` | duration | Max time to wait for timeout packets to accumulate before flushing |
| `timeout_batch_concurrency` | int | Max concurrent timeout batches being processed |
| `should_relay_success_acks` | bool | Whether to relay acknowledgements for successful packet deliveries |
| `should_relay_error_acks` | bool | Whether to relay acknowledgements for failed packet deliveries |

#### `chains.<chain_key>.signer_gas_alert_thresholds`

| Field | Type | Description |
|-------|------|-------------|
| `ibcv2.warning_threshold` | string | Gas balance (in smallest denom) at which the metric reports warning state |
| `ibcv2.critical_threshold` | string | Gas balance (in smallest denom) at which the metric reports critical state |

## Signing

The relayer supports two signing modes, configured via the `signing` block in the YAML config. The mode is inferred from which fields are populated:

- If `grpc_address` is set → **remote signing** (ignores `keys_path`)
- Else if `keys_path` is set → **local signing** from key file
- Else → fatal error at startup

### Local Signing

Set `signing.keys_path` to point to a JSON file containing private keys. The format is a map of chain IDs to key objects:

```yaml
signing:
  keys_path: "./config/local/ibcv2keys.json"
```

```json
{
  "1": {
    "private_key": "0xabc123..."
  },
  "cosmoshub-4": {
    "private_key": "abc123..."
  }
}
```

For EVM chains, the private key is a hex-encoded ECDSA private key. For Cosmos chains, it is a hex-encoded secp256k1 private key.

### Remote Signing

For production deployments, the relayer can delegate signing to an external gRPC service. This keeps private keys isolated from the relayer process.

For the full integration spec — per-chain signing contracts, `GetWallet` semantics, authentication behavior, common pitfalls, and trust assumptions — see [`docs/remote-signing.md`](./docs/remote-signing.md).

#### Configuration

```yaml
signing:
  grpc_address: "localhost:50052"
  cosmos_wallet_key: "my-cosmos-wallet"
  evm_wallet_key: "my-evm-wallet"
```

#### Authentication

The remote signer connection uses the `SERVICE_ACCOUNT_TOKEN` environment variable as a bearer token in gRPC metadata for authenticating requests to the signing service.

#### Signer Service Interface

The remote signer must implement the following gRPC service:

```proto
service SignerService {
    rpc GetWallet(GetWalletRequest) returns (GetWalletResponse) {}
    rpc Sign(SignRequest) returns (SignResponse) {}
}
```

The `Sign` RPC accepts transaction payloads for EVM, Cosmos, and Solana chains and returns the appropriate signature format for each:

- **EVM**: Accepts serialized tx bytes + chain ID, returns `(r, s, v)` signature components
- **Cosmos**: Accepts sign doc bytes, returns a raw signature

The full proto definition is at [`proto/signer/signerservice.proto`](proto/signer/signerservice.proto).

#### Implementing a Compatible Signer Service

To implement your own signer service, refer to the proto file for message formats and signature requirements.

**Methods used by the relayer:**
- `GetWallet` - called at startup to retrieve public keys (uses `Cosmos`, `Ethereum`, and `Solana` pubkey types)
- `Sign` - called for every transaction

**Sign payload types used by the relayer:**
- `EvmTransaction` / `EvmTransactionSignature`
- `CosmosTransaction` / `CosmosTransactionSignature`

**Sign payload types not used by the relayer:**
- `RawMessage` / `RawMessageSignature`
- `SolanaTransaction` / `SolanaTransactionSignature`


**Authentication:** The relayer sends the `SERVICE_ACCOUNT_TOKEN` environment variable as a bearer token in the `authorization` gRPC metadata header. Validation is optional.

## Security

To report a vulnerability, see [SECURITY.md](./SECURITY.md). Do not file public GitHub issues for security reports.
