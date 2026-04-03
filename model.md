# Bitcoin Blockchain Data Model for Neo4j

A dual-layer graph model for storing raw Bitcoin blockchain data in Neo4j, designed for both financial crime investigation and blockchain forensics.

---

## Design Principles

1. **Raw data only** - Only store data that exists in the blockchain or can be deterministically derived from it. No external enrichment, clustering, or risk scoring in this layer.

2. **Dual-layer approach** - Provide both a simplified "follow the money" layer for investigations and a detailed UTXO layer for forensic analysis, connected through shared Transaction nodes.

3. **Semantic relationship direction** - Relationships flow in the direction of value movement. Inputs flow into transactions, outputs flow out.

---

## Blockchain Data Mapping

This model is designed to ingest data from Bitcoin Core raw block files (`.blk` files). Here's how blockchain data maps to our graph model:

### Block Data → Block Node
Raw block headers map directly to Block node properties:
- `height`, `hash`, `previousHash`, `merkleRoot`, `timestamp`, `bits`, `difficulty`, `nonce`, `version` → Direct mapping from block header
- `txCount` → Count of transactions in block
- `size`, `weight` → Calculated from block data

### Transaction Data → Transaction Node
Raw transaction data from blocks:
- `txid`, `version`, `locktime`, `size`, `vsize`, `weight` → Direct mapping from transaction
- `blockHeight`, `blockHash`, `timestamp` → Inherited from containing block
- `totalInput` → **Calculated in Rust**: Sum of all input amounts in satoshis (looked up via UTXO cache with Neo4j fallback)
- `totalOutput` → **Calculated in Rust**: Sum of all output amounts in satoshis
- `fee` → **Calculated in Rust**: `totalInput - totalOutput` (zero for coinbase transactions)
- `isCoinbase` → **Derived**: True if transaction has exactly one input with no previous output reference

All amounts are stored as integers in **satoshis** (1 BTC = 100,000,000 satoshis).

### Output Data → Output Node
Raw transaction output (vout) data:
- `outputId` → **Derived**: Concatenation of `{txid}:{outputIndex}`
- `outputIndex`, `amount`, `scriptPubKey` → Direct mapping from vout
- `scriptType` → **Derived**: Parsed from scriptPubKey (see [ADDRESS_DERIVATION.md](../bitcoin/ADDRESS_DERIVATION.md))
- `isSpent`, `spentInTxid`, `spentAtHeight` → **State tracking**: Updated when output is later consumed by an input

### Input Data → Input Node
Raw transaction input (vin) data:
- `inputId` → **Derived**: Concatenation of `{txid}:{inputIndex}`
- `inputIndex`, `scriptSig`, `sequence` → Direct mapping from vin, stored as node properties
- `witness` → Direct mapping from vin.txinwitness (SegWit transactions only)
- `previousTxid`, `previousOutputIndex` → Used to create the SPENDS relationship (not stored as node properties)

### Address Data → Address Node
**Not present in raw blockchain** - Addresses are **deterministically derived** by:
1. Parsing the `scriptPubKey` from each output
2. Extracting the address based on script type (P2PKH, P2SH, P2WPKH, P2WSH, P2TR, P2PK)
3. For details see [ADDRESS_DERIVATION.md](../bitcoin/ADDRESS_DERIVATION.md)

### Relationship Data
- `HAS_INPUT`, `HAS_OUTPUT`, `INCLUDED_IN`, `LOCKED_TO` → **Directly derivable** from transaction structure
- `SPENDS` → **Requires lookup**: Match `input.previousTxid:previousOutputIndex` to existing Output nodes
- `PERFORMS` → **Derived**: Conceptually follows `Input → SPENDS → Output → LOCKED_TO → Address` to find sender (pre-aggregated in Rust, not graph-traversed)
- `BENEFITS_TO` → **Derived**: Conceptually follows `Output → LOCKED_TO → Address` to find recipient (pre-aggregated in Rust, not graph-traversed)
- `NEXT_BLOCK` → **Derived**: Link blocks by sequential height

---

## Conceptual Overview

```
╔═══════════════════════════════════════════════════════════════════════════════════════╗
║  SIMPLIFIED LAYER (Follow the Money)                                                  ║
║                                                                                       ║
║     Alice ──PERFORMS──► Transaction ──BENEFITS_TO──► Bob                              ║
║                                     ──BENEFITS_TO──► Alice (change)                   ║
║                                                                                       ║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║  DETAILED LAYER (UTXO Mechanics)                                                      ║
║                                                                                       ║
║              Previous Output ──LOCKED_TO──► Alice                                     ║
║                      │                                                                ║
║                   SPENDS                                                              ║
║                      │                                                                ║
║                      ▼                                                                ║
║                   Input 0 ◄──HAS_INPUT── Transaction ──HAS_OUTPUT──► Output 0         ║
║                                                       ──HAS_OUTPUT──► Output 1         ║
║                                                                          │             ║
║                                                                          │             ║
║                                                          Output 0 ──LOCKED_TO──► Bob  ║
║                                                          Output 1 ──LOCKED_TO──► Alice║
║                                                                                       ║
╚═══════════════════════════════════════════════════════════════════════════════════════╝
```

The Transaction node is shared between both layers. This allows simple money-flow queries while preserving the ability to drill into UTXO-level detail when needed.

---

## Why Two Layers?

**Simplified layer** answers: "Who sent money to whom?"
- Fast traversal for following funds across multiple hops
- Pattern matching for financial crime investigation
- Clean Address → Transaction → Address paths

**Detailed layer** answers: "What exactly happened in this transaction?"
- Which specific UTXOs were consumed (inputs)
- Which new UTXOs were created (outputs)
- Cryptographic proof data (scriptSig, witness)
- Precise UTXO tracking for balance calculation

---

## Node Definitions

### Address

A Bitcoin address that can send or receive funds.

| Property | Type | Description |
|----------|------|-------------|
| address | STRING | Bitcoin address (e.g., `1A1zP1...`, `bc1q...`, `bc1p...`) |

Note: Address nodes are derived by parsing the scriptPubKey of outputs. They don't exist explicitly in raw blockchain data but are deterministically derivable. The address type (P2PKH, P2SH, etc.) is encoded in the address format itself and can be determined from the corresponding Output's `scriptType` property via the LOCKED_TO relationship.

---

### Transaction

A Bitcoin transaction that moves value.

| Property | Type | Description |
|----------|------|-------------|
| txid | STRING | Transaction hash (unique identifier) |
| blockHeight | INTEGER | Block number containing this transaction |
| blockHash | STRING | Hash of the containing block |
| timestamp | DATETIME | Block timestamp |
| totalInput | INTEGER | Sum of all input amounts (satoshis) |
| totalOutput | INTEGER | Sum of all output amounts (satoshis) |
| fee | INTEGER | Miner fee: totalInput - totalOutput (satoshis) |
| size | INTEGER | Transaction size in bytes |
| vsize | INTEGER | Virtual size (SegWit) |
| weight | INTEGER | Transaction weight units |
| version | INTEGER | Transaction version |
| locktime | INTEGER | Locktime value |
| isCoinbase | BOOLEAN | True if this is a coinbase (mining reward) transaction |

---

### Output

A transaction output representing a "coin" that can be spent once.

| Property | Type | Description |
|----------|------|-------------|
| outputId | STRING | Unique identifier: `{txid}:{outputIndex}` |
| outputIndex | INTEGER | Position in transaction outputs (0, 1, 2...) |
| amount | INTEGER | Amount in satoshis (1 BTC = 100,000,000 satoshis) |
| scriptPubKey | STRING | Locking script (defines who can spend) |
| scriptType | STRING | Script type: `P2PKH`, `P2SH`, `P2WPKH`, `P2WSH`, `P2TR`, `P2PK`, `NULL_DATA`, `UNKNOWN` |
| isSpent | BOOLEAN | Has this output been spent? |
| spentInTxid | STRING | Transaction ID that spent this output (null if unspent) |
| spentAtHeight | INTEGER | Block height when spent (null if unspent) |

An unspent output is called a UTXO (Unspent Transaction Output). The set of all UTXOs represents all spendable Bitcoin.

---

### Input

A transaction input that spends a previous output.

| Property | Type | Description |
|----------|------|-------------|
| inputId | STRING | Unique identifier: `{txid}:{inputIndex}` |
| inputIndex | INTEGER | Position in transaction inputs (0, 1, 2...) |
| scriptSig | STRING | Unlocking script (proves authorisation to spend) |
| sequence | INTEGER | Sequence number |
| witness | [STRING] | Witness data array (SegWit transactions) |

Coinbase transactions have a single input with no previous output reference.

**Note:** The `previousTxid` and `previousOutputIndex` from the raw blockchain data are used during ingestion to create the SPENDS relationship to the referenced Output node, but are not stored as properties on the Input node itself. The SPENDS relationship encodes this link structurally.

---

### Block

A block in the blockchain.

| Property | Type | Description |
|----------|------|-------------|
| height | INTEGER | Block number (0 = genesis) |
| hash | STRING | Block hash |
| previousHash | STRING | Hash of previous block |
| merkleRoot | STRING | Merkle root of transactions |
| timestamp | DATETIME | When block was mined |
| txCount | INTEGER | Number of transactions |
| size | INTEGER | Block size in bytes |
| weight | INTEGER | Block weight (SegWit) |
| bits | STRING | Difficulty target (compact) |
| difficulty | FLOAT | Mining difficulty |
| nonce | INTEGER | Mining nonce |
| version | INTEGER | Block version |

---

### IngestionCheckpoint

Tracks the progress of blockchain ingestion to enable resume-on-failure.

| Property | Type | Description |
|----------|------|-------------|
| lastProcessedHeight | INTEGER | Last successfully ingested block height |
| lastProcessedHash | STRING | Hash of last processed block (for verification) |
| lastProcessedFile | STRING | Name of `.blk` file being processed (e.g., "blk00000.dat") |
| lastProcessedFileOffset | INTEGER | Byte offset within the file (optional optimization) |
| timestamp | DATETIME | When checkpoint was last updated |
| status | STRING | Current status: `in_progress`, `completed`, `paused`, `error` |

**Purpose**: Allows ingestion to resume from the last successfully processed block after a failure or interruption. Since each block is ingested in a single Neo4j transaction, at most one block needs to be reprocessed on resume.

**Sentinel value**: The initial `lastProcessedHeight` is set to `-999` (not `-1`) to work around a neo4rs driver bug that misreads `-1` as `255`. A value of `-999` means "not yet started".

**Note**: This is metadata for the ingestion process, not part of the blockchain data itself. It can be safely deleted and recreated without affecting the blockchain graph.

---

## Relationship Definitions

### Simplified Layer (Follow the Money)

| Relationship | Direction | Properties | Description |
|--------------|-----------|------------|-------------|
| PERFORMS | `(:Address)-[:PERFORMS]->(:Transaction)` | `inputCount` (INTEGER), `amountSpent` (INTEGER, satoshis) | Address whose UTXO was spent as an input |
| BENEFITS_TO | `(:Transaction)-[:BENEFITS_TO]->(:Address)` | `outputCount` (INTEGER), `amountReceived` (INTEGER, satoshis) | Address that received an output |

These relationships are pre-aggregated in Rust during ingestion. Each relationship aggregates all inputs/outputs between a given address and transaction into a single relationship with totals.

---

### Detailed Layer (UTXO Mechanics)

| Relationship | Direction | Description |
|--------------|-----------|-------------|
| HAS_INPUT | `(:Transaction)-[:HAS_INPUT]->(:Input)` | Transaction has this input |
| HAS_OUTPUT | `(:Transaction)-[:HAS_OUTPUT]->(:Output)` | Transaction produces output |
| SPENDS | `(:Input)-[:SPENDS]->(:Output)` | Input consumes a previous output |
| LOCKED_TO | `(:Output)-[:LOCKED_TO]->(:Address)` | Output is controlled by address |

---

### Block Structure

| Relationship | Direction | Description |
|--------------|-----------|-------------|
| INCLUDED_IN | `(:Transaction)-[:INCLUDED_IN]->(:Block)` | Transaction is in this block |
| NEXT_BLOCK | `(:Block)-[:NEXT_BLOCK]->(:Block)` | Links blocks in chain order |

---

## Complete Relationship Diagram

```
                                    NEXT_BLOCK
                              ┌───────────────────┐
                              │                   │
                              ▼                   │
                           Block N ◄────────── Block N+1
                              ▲
                              │
                         INCLUDED_IN
                              │
┌─────────────────────────────┴─────────────────────────────┐
│                                                           │
│                       Transaction                         │
│                                                           │
│    ┌──────────────────────┬───────────────────────┐       │
│    │                      │                       │       │
│    │    PERFORMS          │          BENEFITS_TO  │       │
│    │    (address whose    │          (address     │       │
│    │    UTXO was spent)   │          receiving    │       │
│    │                      │          output)      │       │
│    ▼                      │                       ▼       │
│  Address                  │                    Address    │
│    ▲                      │                       ▲       │
│    │                      │                       │       │
│    │ LOCKED_TO            │            LOCKED_TO  │       │
│    │                      │                       │       │
│  Output ◄────── SPENDS ── Input ◄── HAS_INPUT ─── Transaction ─── HAS_OUTPUT ──► Output
│ (previous)                                                                          │
│                                                                                     │
│                                                                                     │
│                                                                          LOCKED_TO  │
│                                                                                     │
│                                                                                  Address
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```
