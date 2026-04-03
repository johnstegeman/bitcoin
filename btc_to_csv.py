#!/usr/bin/env python3
"""
Bitcoin blockchain → Neo4j CSV exporter.

Connects to Bitcoin Core via JSON-RPC and writes CSV files for use with
neo4j-admin database import. One file per node label, one per relationship type.
Headers are written to separate *-header.csv files (no header row in data files).

Usage:
    python btc_to_csv.py [--start HEIGHT] [--end HEIGHT] [--resume]

Prerequisites:
    pip install -r requirements.txt
    Configure RPC_* constants below.

Notes:
  - Start from block 0 (genesis) for correct fee/totalInput calculations.
    The SQLite UTXO cache accumulates all unspent outputs as you go.
  - Output.isSpent is written as false for all outputs. After import, run
    post_import.cypher to populate isSpent/spentInTxid/spentAtHeight via
    the structural SPENDS relationships.
  - Address nodes may be written more than once (same address appears in many
    outputs). Use --ignore-duplicate-nodes=true on neo4j-admin import.
  - PERFORMS relationships are only written when the spent UTXO's scriptPubKey
    has a decodable single address (P2PKH, P2SH, P2WPKH, P2WSH, P2TR, P2PK).
    Multisig and OP_RETURN inputs are skipped for PERFORMS but SPENDS is still written.
"""

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

# ─── Configuration ────────────────────────────────────────────────────────────

RPC_HOST     = "192.168.1.100"   # IP/hostname of your Bitcoin Core node
RPC_PORT     = 8332
RPC_USER     = "bitcoinrpc"
RPC_PASSWORD = "your_rpc_password_here"

OUTPUT_DIR   = Path("./bitcoin_csv")

# Commit SQLite and flush CSV writers every N blocks
COMMIT_EVERY = 100

# Print progress every N blocks
LOG_EVERY = 1000


# ─── RPC Client ───────────────────────────────────────────────────────────────

class BitcoinRPC:
    def __init__(self):
        self.url     = f"http://{RPC_HOST}:{RPC_PORT}/"
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(RPC_USER, RPC_PASSWORD)
        self._req_id = 0

    def _call(self, method: str, *params):
        self._req_id += 1
        resp = self.session.post(
            self.url,
            json={"jsonrpc": "1.1", "id": self._req_id, "method": method, "params": list(params)},
            timeout=300,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(f"RPC {method} error: {data['error']}")
        return data["result"]

    def getblockcount(self) -> int:
        return self._call("getblockcount")

    def getblockhash(self, height: int) -> str:
        return self._call("getblockhash", height)

    def getblock(self, block_hash: str, verbosity: int = 2) -> dict:
        # verbosity=2 returns the block with full decoded transaction data inline.
        # This avoids per-transaction RPC calls.
        return self._call("getblock", block_hash, verbosity)


# ─── SQLite UTXO Cache + Checkpoint ───────────────────────────────────────────

def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), isolation_level=None)  # manual transactions
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-524288")  # 512 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS utxo (
            txid    TEXT    NOT NULL,
            vout    INTEGER NOT NULL,
            amount  INTEGER NOT NULL,
            address TEXT,
            PRIMARY KEY (txid, vout)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS checkpoint (
            id           INTEGER PRIMARY KEY CHECK (id = 1),
            last_height  INTEGER NOT NULL DEFAULT -1
        );
        INSERT OR IGNORE INTO checkpoint (id, last_height) VALUES (1, -1);
    """)
    return conn


def get_checkpoint(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT last_height FROM checkpoint WHERE id = 1").fetchone()
    return row[0] if row else -1


def save_checkpoint(conn: sqlite3.Connection, height: int):
    conn.execute("UPDATE checkpoint SET last_height = ? WHERE id = 1", (height,))


# ─── CSV Header Definitions ───────────────────────────────────────────────────
# These are written once to *-header.csv files (separate from the data CSVs).
# neo4j-admin import column type annotations:
#   :ID(space), :START_ID(space), :END_ID(space)
#   :int, :long, :float, :boolean, :datetime, :string (default), :string[]

HEADERS: dict[str, list[str]] = {
    "nodes_block": [
        "hash:ID(Block)",
        "height:int",
        "previousHash:string",
        "merkleRoot:string",
        "timestamp:datetime",
        "txCount:int",
        "size:long",
        "weight:long",
        "bits:string",
        "difficulty:float",
        "nonce:long",
        "version:int",
    ],
    "nodes_transaction": [
        "txid:ID(Transaction)",
        "blockHeight:int",
        "blockHash:string",
        "timestamp:datetime",
        "totalInput:long",
        "totalOutput:long",
        "fee:long",
        "size:int",
        "vsize:int",
        "weight:int",
        "version:int",
        "locktime:long",
        "isCoinbase:boolean",
    ],
    "nodes_output": [
        "outputId:ID(Output)",
        "outputIndex:int",
        "amount:long",
        "scriptPubKey:string",
        "scriptType:string",
        "isSpent:boolean",
        "spentInTxid:string",
        "spentAtHeight:int",
    ],
    "nodes_input": [
        "inputId:ID(Input)",
        "inputIndex:int",
        "scriptSig:string",
        "sequence:long",
        "witness:string[]",
    ],
    "nodes_address": [
        "address:ID(Address)",
    ],
    # Relationships
    "rels_next_block":  [":START_ID(Block)",       ":END_ID(Block)"],
    "rels_included_in": [":START_ID(Transaction)", ":END_ID(Block)"],
    "rels_has_input":   [":START_ID(Transaction)", ":END_ID(Input)"],
    "rels_has_output":  [":START_ID(Transaction)", ":END_ID(Output)"],
    "rels_spends":      [":START_ID(Input)",        ":END_ID(Output)"],
    "rels_locked_to":   [":START_ID(Output)",       ":END_ID(Address)"],
    "rels_performs":    [":START_ID(Address)",      ":END_ID(Transaction)", "inputCount:int", "amountSpent:long"],
    "rels_benefits_to": [":START_ID(Transaction)", ":END_ID(Address)",     "outputCount:int", "amountReceived:long"],
}


def write_headers(output_dir: Path):
    """Write all *-header.csv files. Overwrites existing headers."""
    for name, cols in HEADERS.items():
        p = output_dir / f"{name}-header.csv"
        with p.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(cols)
    print(f"Headers written to {output_dir}/")


# ─── Helpers ──────────────────────────────────────────────────────────────────

_SCRIPT_TYPE_MAP = {
    "pubkey":                "P2PK",
    "pubkeyhash":            "P2PKH",
    "scripthash":            "P2SH",
    "witness_v0_keyhash":    "P2WPKH",
    "witness_v0_scripthash": "P2WSH",
    "witness_v1_taproot":    "P2TR",
    "nulldata":              "NULL_DATA",
}


def map_script_type(spk: dict) -> str:
    return _SCRIPT_TYPE_MAP.get(spk.get("type", ""), "UNKNOWN")


def get_address(spk: dict) -> Optional[str]:
    """
    Extract single address from scriptPubKey if available.
    Bitcoin Core (>= 22.0) returns 'address' for single-address scripts.
    Older versions used 'addresses' list; we handle both.
    Returns None for multisig, OP_RETURN, and unrecognised scripts.
    """
    if addr := spk.get("address"):
        return addr
    addrs = spk.get("addresses", [])
    return addrs[0] if len(addrs) == 1 else None


def to_satoshis(btc_float: float) -> int:
    """Convert BTC float to integer satoshis without floating-point error."""
    return int(round(btc_float * 100_000_000))


def fmt_datetime(unix_ts: int) -> str:
    """Unix timestamp → ISO 8601 UTC string for Neo4j datetime type."""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def encode_witness(witness: list) -> str:
    """Encode SegWit witness stack as semicolon-delimited hex strings."""
    return ";".join(witness) if witness else ""


# ─── Block Processor ──────────────────────────────────────────────────────────

def process_block(block: dict, w: dict, db: sqlite3.Connection):
    """
    Process one block: write all nodes and relationships to CSV writers.

    Two-pass approach within each block:
      Pass 1 – cache all outputs in this block's transactions into SQLite
               so that intra-block spends (tx[i] spending tx[j<i]'s output)
               are resolvable in Pass 2.
      Pass 2 – process inputs: look up spent UTXOs for amount/address,
               compute totalInput/fee, write all CSV rows.

    SQLite is updated atomically (BEGIN before this function, COMMIT after).
    """
    height     = block["height"]
    block_hash = block["hash"]
    prev_hash  = block.get("previousblockhash", "")
    time_str   = fmt_datetime(block["time"])

    # ── Block node ──────────────────────────────────────────────────────────
    w["nodes_block"].writerow([
        block_hash,
        height,
        prev_hash,
        block["merkleroot"],
        time_str,
        block["nTx"],
        block["size"],
        block.get("weight", 0),
        block["bits"],
        block["difficulty"],
        block["nonce"],
        block["version"],
    ])

    # ── NEXT_BLOCK relationship (skip for genesis) ──────────────────────────
    if height > 0:
        w["rels_next_block"].writerow([prev_hash, block_hash])

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 1: Process all outputs. Cache them in SQLite so intra-block inputs
    #         can look them up in Pass 2.
    # ─────────────────────────────────────────────────────────────────────────
    utxo_inserts: list[tuple] = []

    for tx in block["tx"]:
        txid = tx["txid"]

        benefits: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        total_output = 0

        for vout in tx["vout"]:
            n          = vout["n"]
            output_id  = f"{txid}:{n}"
            spk        = vout["scriptPubKey"]
            amount     = to_satoshis(vout["value"])
            script_hex = spk.get("hex", "")
            stype      = map_script_type(spk)
            address    = get_address(spk)
            total_output += amount

            # Output node (isSpent=false; updated by post_import.cypher)
            w["nodes_output"].writerow([
                output_id, n, amount, script_hex, stype,
                "false", "", "",   # isSpent, spentInTxid, spentAtHeight
            ])
            w["rels_has_output"].writerow([txid, output_id])

            if address:
                w["nodes_address"].writerow([address])
                w["rels_locked_to"].writerow([output_id, address])
                rec = benefits[address]
                rec[0] += 1
                rec[1] += amount

            utxo_inserts.append((txid, n, amount, address))

        # BENEFITS_TO – one relationship per unique recipient address per tx
        for addr, (cnt, received) in benefits.items():
            w["rels_benefits_to"].writerow([txid, addr, cnt, received])

        # Stash per-tx data needed for Pass 2
        tx["_total_output"] = total_output

    # Batch-insert all outputs into UTXO cache (still inside the open transaction)
    db.executemany(
        "INSERT OR REPLACE INTO utxo (txid, vout, amount, address) VALUES (?, ?, ?, ?)",
        utxo_inserts,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # PASS 2: Process inputs. Look up spent UTXOs, write Input nodes and
    #         transaction-level rows (Transaction node, INCLUDED_IN, PERFORMS).
    # ─────────────────────────────────────────────────────────────────────────
    utxo_deletes: list[tuple] = []

    for tx in block["tx"]:
        txid        = tx["txid"]
        is_coinbase = "coinbase" in tx["vin"][0]
        total_output = tx["_total_output"]

        performs: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        total_input = 0

        for idx, vin in enumerate(tx["vin"]):
            input_id  = f"{txid}:{idx}"
            sequence  = vin["sequence"]
            witness   = encode_witness(vin.get("txinwitness", []))

            if is_coinbase:
                script_sig = vin.get("coinbase", "")
                # Coinbase input node (no SPENDS relationship)
                w["nodes_input"].writerow([input_id, idx, script_sig, sequence, witness])
                w["rels_has_input"].writerow([txid, input_id])
            else:
                script_sig = vin.get("scriptSig", {}).get("hex", "")
                prev_txid  = vin["txid"]
                prev_vout  = vin["vout"]
                prev_out_id = f"{prev_txid}:{prev_vout}"

                # Look up spent UTXO for amount and sender address
                row = db.execute(
                    "SELECT amount, address FROM utxo WHERE txid = ? AND vout = ?",
                    (prev_txid, prev_vout),
                ).fetchone()

                if row:
                    amt, addr = row
                    total_input += amt
                    if addr:
                        rec = performs[addr]
                        rec[0] += 1
                        rec[1] += amt
                else:
                    # Only expected if --start-height > 0 (missing genesis UTXO history)
                    print(
                        f"  WARN block {block['height']}: UTXO not found {prev_txid}:{prev_vout}",
                        file=sys.stderr,
                    )

                utxo_deletes.append((prev_txid, prev_vout))

                w["nodes_input"].writerow([input_id, idx, script_sig, sequence, witness])
                w["rels_has_input"].writerow([txid, input_id])
                w["rels_spends"].writerow([input_id, prev_out_id])

        # Coinbase: totalInput = 0, fee = 0 (reward is not counted as "input")
        if is_coinbase:
            total_input = 0
            fee = 0
        else:
            fee = total_input - total_output

        # Transaction node
        w["nodes_transaction"].writerow([
            txid,
            height,
            block_hash,
            time_str,
            total_input,
            total_output,
            fee,
            tx["size"],
            tx.get("vsize", tx["size"]),
            tx.get("weight", tx["size"] * 4),
            tx["version"],
            tx["locktime"],
            "true" if is_coinbase else "false",
        ])
        w["rels_included_in"].writerow([txid, block_hash])

        # PERFORMS – one relationship per unique sender address per tx
        for addr, (cnt, spent) in performs.items():
            w["rels_performs"].writerow([addr, txid, cnt, spent])

    # Delete spent UTXOs from cache (keeps the DB lean: only unspent outputs remain)
    db.executemany(
        "DELETE FROM utxo WHERE txid = ? AND vout = ?",
        utxo_deletes,
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Export Bitcoin blockchain to Neo4j CSV files via RPC.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", type=int, default=None,
                        help="Start block height (default: resume from checkpoint, or 0)")
    parser.add_argument("--end",   type=int, default=None,
                        help="End block height inclusive (default: current chain tip)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last successful checkpoint")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    db = open_db(OUTPUT_DIR / "utxo_cache.db")
    checkpoint = get_checkpoint(db)

    # Determine start height
    if args.start is not None:
        start = args.start
        if args.start > 0:
            print(
                f"WARNING: Starting from block {args.start} (not genesis). "
                "UTXO cache will be incomplete; totalInput/fee will be wrong for "
                "transactions that spend outputs from blocks 0.." + str(args.start - 1) + ".",
                file=sys.stderr,
            )
    elif args.resume and checkpoint >= 0:
        start = checkpoint + 1
        print(f"Resuming from block {start} (last checkpoint: {checkpoint})")
    else:
        start = 0

    rpc = BitcoinRPC()

    # Determine end height
    if args.end is not None:
        end = args.end
    else:
        end = rpc.getblockcount()
        print(f"Chain tip: block {end:,}")

    if start > end:
        print(f"Nothing to do: start={start} > end={end}")
        return

    print(f"Processing blocks {start:,} → {end:,}  ({end - start + 1:,} blocks)")

    # Write header files (always regenerate; safe to overwrite)
    write_headers(OUTPUT_DIR)

    # Open data CSV files. Use append mode when resuming so we can continue
    # writing to existing files without reprocessing earlier blocks.
    open_mode = "a" if (args.resume and checkpoint >= 0) else "w"
    csv_files = {}
    writers   = {}
    for name in HEADERS:
        path = OUTPUT_DIR / f"{name}.csv"
        csv_files[name] = open(path, open_mode, newline="", encoding="utf-8", buffering=1 << 20)
        writers[name]   = csv.writer(csv_files[name], quoting=csv.QUOTE_MINIMAL)

    current_height = start
    try:
        db.execute("BEGIN")

        for height in range(start, end + 1):
            current_height = height
            block_hash = rpc.getblockhash(height)
            block      = rpc.getblock(block_hash, verbosity=2)

            process_block(block, writers, db)
            save_checkpoint(db, height)

            # Periodic commit + flush
            if height % COMMIT_EVERY == 0 or height == end:
                db.execute("COMMIT")
                for f in csv_files.values():
                    f.flush()
                if height < end:
                    db.execute("BEGIN")

            if height % LOG_EVERY == 0 or height == end:
                pct = (height - start + 1) / (end - start + 1) * 100
                print(f"  block {height:>9,} / {end:,}  ({pct:5.1f}%)")

    except KeyboardInterrupt:
        print(f"\nInterrupted at block {current_height}. Committing checkpoint…")
        try:
            db.execute("COMMIT")
        except Exception:
            pass
        print(f"Checkpoint saved at block {get_checkpoint(db)}. Re-run with --resume to continue.")
    except Exception as exc:
        print(f"\nError at block {current_height}: {exc}", file=sys.stderr)
        try:
            db.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        for f in csv_files.values():
            f.close()
        db.close()

    print(f"\nDone. CSV files written to {OUTPUT_DIR}/")
    print("Next steps:")
    print("  1. Run import.sh to load into Neo4j")
    print("  2. Run post_import.cypher to populate Output.isSpent fields")


if __name__ == "__main__":
    main()
