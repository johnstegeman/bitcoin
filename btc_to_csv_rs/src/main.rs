/*!
 * Bitcoin blockchain → Neo4j CSV exporter (Rust, reads raw .blk files)
 *
 * Two-phase approach:
 *   Phase 1 – Scan all blk*.dat files, build a block index in SQLite
 *              (hash → file position). Identify the main chain by following
 *              prev_blockhash links from genesis.
 *   Phase 2 – Walk the main chain in height order. For each block, seek
 *              directly to its position in the blk file and parse it.
 *              Within each block, use a two-pass algorithm:
 *                Pass 1: write Output/Address nodes, cache UTXOs in SQLite.
 *                Pass 2: look up spent UTXOs, compute fees, write Input/
 *                        Transaction nodes and all relationships.
 *
 * Usage:
 *   cargo build --release
 *   ./target/release/btc_to_csv \
 *       --blocks-dir ~/.bitcoin/blocks \
 *       --output-dir ./bitcoin_csv
 *
 * After export:
 *   ../import.sh ./bitcoin_csv
 *   cypher-shell < ../post_import.cypher
 */

use itoa::Buffer as ItoaBuf;
use rocksdb::{Options as RocksOptions, WriteBatch, DB as RocksDB};
use rustc_hash::{FxHashMap, FxHashSet};
use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use bitcoin::{
    block::Header as BlockHeader,
    consensus::Decodable,
    hashes::Hash,
    script::Instruction,
    Address, Block, Network, PublicKey,
};
use chrono::{TimeZone, Utc};
use clap::Parser;
use rusqlite::{params, Connection, OptionalExtension};

// ─── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "btc_to_csv", about = "Export Bitcoin blockchain to Neo4j CSV files")]
struct Args {
    /// Directory containing Bitcoin Core blk*.dat block files
    #[arg(long, default_value = "~/.bitcoin/blocks")]
    blocks_dir: String,

    /// Directory to write CSV output files
    #[arg(long, default_value = "./bitcoin_csv")]
    output_dir: String,

    /// Start block height (default: resume from checkpoint, or 0)
    #[arg(long)]
    start: Option<i64>,

    /// End block height inclusive (default: end of indexed chain)
    #[arg(long)]
    end: Option<i64>,

    /// Force re-index (re-scan all .blk files even if index already exists)
    #[arg(long, default_value = "false")]
    reindex: bool,
}

// ─── Constants ────────────────────────────────────────────────────────────────

const MAINNET_MAGIC: [u8; 4] = [0xf9, 0xbe, 0xb4, 0xd9];
const GENESIS_HASH: &str =
    "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";

// ─── Path helpers ─────────────────────────────────────────────────────────────

fn expand_path(s: &str) -> PathBuf {
    if let Some(rest) = s.strip_prefix("~/") {
        let home = std::env::var("HOME").unwrap_or_default();
        PathBuf::from(home).join(rest)
    } else {
        PathBuf::from(s)
    }
}

fn blk_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("blk") && n.ends_with(".dat"))
                .unwrap_or(false)
        })
        .collect();
    files.sort(); // blk00000.dat, blk00001.dat …
    Ok(files)
}

// ─── Datetime formatting ──────────────────────────────────────────────────────

fn fmt_datetime(unix: u32) -> String {
    Utc.timestamp_opt(unix as i64, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

// ─── Script type + address ────────────────────────────────────────────────────

fn script_type(script: &bitcoin::Script) -> &'static str {
    if script.is_p2pkh()           { "P2PKH"     }
    else if script.is_p2sh()       { "P2SH"      }
    else if script.is_p2wpkh()     { "P2WPKH"    }
    else if script.is_p2wsh()      { "P2WSH"     }
    else if script.is_p2tr()       { "P2TR"      }
    else if script.is_p2pk()       { "P2PK"      }
    else if script.is_op_return()  { "NULL_DATA" }
    else                           { "UNKNOWN"   }
}

/// Derive a single address string from a scriptPubKey, if possible.
/// Returns None for multisig, OP_RETURN, and unrecognised scripts.
fn script_address(script: &bitcoin::Script) -> Option<String> {
    // Standard single-address scripts (P2PKH, P2SH, P2WPKH, P2WSH, P2TR)
    if let Ok(addr) = Address::from_script(script, Network::Bitcoin) {
        return Some(addr.to_string());
    }
    // P2PK: not handled by from_script; extract pubkey and derive P2PKH address.
    if script.is_p2pk() {
        let mut instr = script.instructions();
        if let Some(Ok(Instruction::PushBytes(data))) = instr.next() {
            if let Ok(pk) = PublicKey::from_slice(data.as_bytes()) {
                // Build a P2PKH script from the pubkey hash, then re-derive address.
                let p2pkh = bitcoin::ScriptBuf::new_p2pkh(&pk.pubkey_hash());
                if let Ok(addr) = Address::from_script(&p2pkh, Network::Bitcoin) {
                    return Some(addr.to_string());
                }
            }
        }
    }
    None
}

/// Encode witness stack items as semicolon-separated hex strings
/// (Neo4j admin import uses `;` as default array delimiter for `string[]`).
fn encode_witness(witness: &bitcoin::Witness) -> String {
    witness
        .iter()
        .map(hex::encode)
        .collect::<Vec<_>>()
        .join(";")
}

/// Difficulty from compact `bits` target encoding.
fn bits_to_difficulty(bits: u32) -> f64 {
    // Genesis difficulty = 1. Genesis bits = 0x1d00ffff → mantissa=65535, exp=29.
    let max_mantissa = 65535.0_f64;
    let max_exp = 29_i32;
    let exp = (bits >> 24) as i32;
    let mantissa = (bits & 0x007f_ffff) as f64;
    if mantissa == 0.0 {
        return 0.0;
    }
    (max_mantissa / mantissa) * 256.0_f64.powi(max_exp - exp)
}

// ─── SQLite setup ─────────────────────────────────────────────────────────────

fn open_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    // synchronous=OFF: no fsyncs (re-computable data, crash-safety not required).
    // wal_autocheckpoint=0: disable the default auto-checkpoint that fires inside
    // every COMMIT call when WAL >= 1000 pages. Without this, our 300MB WAL was
    // being flushed to disk inside COMMIT (5s stall), while our explicit
    // wal_checkpoint(RESTART) found nothing left to do (0ms). With auto-checkpoint
    // disabled, COMMIT is fast and our explicit RESTART checkpoint does the work
    // as a simple memcpy into the mmap'd DB (~100ms with synchronous=OFF).
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA wal_autocheckpoint=0; PRAGMA cache_size=-524288; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=17179869184;")?;
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS block_idx (
            hash        TEXT NOT NULL PRIMARY KEY,
            prev_hash   TEXT NOT NULL,
            file_path   TEXT NOT NULL,
            byte_offset INTEGER NOT NULL,
            block_size  INTEGER NOT NULL,
            height      INTEGER             -- NULL until chain traversal
        );
        CREATE INDEX IF NOT EXISTS idx_block_idx_prev ON block_idx(prev_hash);
        CREATE INDEX IF NOT EXISTS idx_block_idx_height ON block_idx(height);

        CREATE TABLE IF NOT EXISTS checkpoint (
            id           INTEGER PRIMARY KEY CHECK (id = 1),
            phase        TEXT    NOT NULL DEFAULT 'index',
            last_height  INTEGER NOT NULL DEFAULT -1
        );
        INSERT OR IGNORE INTO checkpoint (id, phase, last_height) VALUES (1, 'index', -1);
    ")?;
    Ok(conn)
}

// ─── RocksDB UTXO store ───────────────────────────────────────────────────────

/// 36-byte key: 32-byte txid || 4-byte vout (big-endian, preserves sort order).
fn utxo_key(txid: &[u8; 32], vout: u32) -> [u8; 36] {
    let mut k = [0u8; 36];
    k[..32].copy_from_slice(txid);
    k[32..].copy_from_slice(&vout.to_be_bytes());
    k
}

/// Value: 8-byte little-endian i64 amount || UTF-8 address (absent = None).
fn utxo_val_encode(amount: i64, addr: &Option<String>) -> Vec<u8> {
    let mut v = amount.to_le_bytes().to_vec();
    if let Some(a) = addr { v.extend_from_slice(a.as_bytes()); }
    v
}

fn utxo_val_decode(b: &[u8]) -> (i64, Option<String>) {
    let amount = i64::from_le_bytes(b[..8].try_into().unwrap());
    let addr = if b.len() > 8 {
        Some(String::from_utf8_lossy(&b[8..]).into_owned())
    } else {
        None
    };
    (amount, addr)
}

fn open_utxo_db(path: &Path) -> Result<RocksDB> {
    let mut opts = RocksOptions::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MB memtable before L0 flush
    opts.set_max_write_buffer_number(4);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    opts.set_level_compaction_dynamic_level_bytes(true);
    Ok(RocksDB::open(&opts, path)?)
}

// ─── Reader thread message ────────────────────────────────────────────────────

struct BlockMsg {
    height:    i64,
    block:     Block,
    hash:      String,
    read_us:   u64,
    decode_us: u64,
}

// ─── Header files ─────────────────────────────────────────────────────────────

const HEADERS: &[(&str, &[&str])] = &[
    ("nodes_block", &[
        "hash:ID(Block)", "height:int", "previousHash:string", "merkleRoot:string",
        "timestamp:datetime", "txCount:int", "size:long", "weight:long",
        "bits:string", "difficulty:float", "nonce:long", "version:int",
    ]),
    ("nodes_transaction", &[
        "txid:ID(Transaction)", "blockHeight:int", "blockHash:string",
        "timestamp:datetime", "totalInput:long", "totalOutput:long", "fee:long",
        "size:int", "vsize:int", "weight:int", "version:int", "locktime:long",
        "isCoinbase:boolean",
    ]),
    ("nodes_output", &[
        "outputId:ID(Output)", "outputIndex:int", "amount:long",
        "scriptPubKey:string", "scriptType:string",
        "isSpent:boolean", "spentInTxid:string", "spentAtHeight:int",
    ]),
    ("nodes_input", &[
        "inputId:ID(Input)", "inputIndex:int", "scriptSig:string",
        "sequence:long", "witness:string[]",
    ]),
    ("nodes_address", &["address:ID(Address)"]),
    // Relationships
    ("rels_next_block",  &[":START_ID(Block)",       ":END_ID(Block)"]),
    ("rels_included_in", &[":START_ID(Transaction)", ":END_ID(Block)"]),
    ("rels_has_input",   &[":START_ID(Transaction)", ":END_ID(Input)"]),
    ("rels_has_output",  &[":START_ID(Transaction)", ":END_ID(Output)"]),
    ("rels_spends",      &[":START_ID(Input)",        ":END_ID(Output)"]),
    ("rels_locked_to",   &[":START_ID(Output)",       ":END_ID(Address)"]),
    ("rels_performs",    &[":START_ID(Address)", ":END_ID(Transaction)", "inputCount:int", "amountSpent:long"]),
    ("rels_benefits_to", &[":START_ID(Transaction)", ":END_ID(Address)", "outputCount:int", "amountReceived:long"]),
];

fn write_headers(output_dir: &Path) -> Result<()> {
    for (name, cols) in HEADERS {
        let path = output_dir.join(format!("{name}-header.csv"));
        let mut w = csv::Writer::from_path(&path)?;
        w.write_record(*cols)?;
        w.flush()?;
    }
    println!("Headers written to {}/", output_dir.display());
    Ok(())
}

// ─── CSV writers ──────────────────────────────────────────────────────────────

struct Writers {
    nodes_block:      csv::Writer<BufWriter<File>>,
    nodes_transaction:csv::Writer<BufWriter<File>>,
    nodes_output:     csv::Writer<BufWriter<File>>,
    nodes_input:      csv::Writer<BufWriter<File>>,
    nodes_address:    csv::Writer<BufWriter<File>>,
    rels_next_block:  csv::Writer<BufWriter<File>>,
    rels_included_in: csv::Writer<BufWriter<File>>,
    rels_has_input:   csv::Writer<BufWriter<File>>,
    rels_has_output:  csv::Writer<BufWriter<File>>,
    rels_spends:      csv::Writer<BufWriter<File>>,
    rels_locked_to:   csv::Writer<BufWriter<File>>,
    rels_performs:    csv::Writer<BufWriter<File>>,
    rels_benefits_to: csv::Writer<BufWriter<File>>,
}

fn open_writer(path: &Path, append: bool) -> Result<csv::Writer<BufWriter<File>>> {
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)?;
    let buf = BufWriter::with_capacity(1 << 23, file); // 8 MB write buffer
    Ok(csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(buf))
}

impl Writers {
    fn open(dir: &Path, append: bool) -> Result<Self> {
        macro_rules! w {
            ($name:literal) => {
                open_writer(&dir.join(concat!($name, ".csv")), append)?
            };
        }
        Ok(Self {
            nodes_block:       w!("nodes_block"),
            nodes_transaction: w!("nodes_transaction"),
            nodes_output:      w!("nodes_output"),
            nodes_input:       w!("nodes_input"),
            nodes_address:     w!("nodes_address"),
            rels_next_block:   w!("rels_next_block"),
            rels_included_in:  w!("rels_included_in"),
            rels_has_input:    w!("rels_has_input"),
            rels_has_output:   w!("rels_has_output"),
            rels_spends:       w!("rels_spends"),
            rels_locked_to:    w!("rels_locked_to"),
            rels_performs:     w!("rels_performs"),
            rels_benefits_to:  w!("rels_benefits_to"),
        })
    }

    fn flush_all(&mut self) -> Result<()> {
        macro_rules! fl {
            ($f:ident) => { self.$f.flush()?; };
        }
        fl!(nodes_block); fl!(nodes_transaction); fl!(nodes_output);
        fl!(nodes_input); fl!(nodes_address);
        fl!(rels_next_block); fl!(rels_included_in);
        fl!(rels_has_input); fl!(rels_has_output);
        fl!(rels_spends); fl!(rels_locked_to);
        fl!(rels_performs); fl!(rels_benefits_to);
        Ok(())
    }
}

// ─── Performance counters ─────────────────────────────────────────────────────

#[derive(Default)]
struct Timings {
    read:         Duration, // read_raw_block (file I/O, overlapped in reader thread)
    decode:       Duration, // consensus_decode (overlapped in reader thread)
    pass1_csv:    Duration, // pass 1: output/address CSV writes
    utxo_insert:  Duration, // in-memory UTXO cache insert (process_block)
    utxo_lookup:  Duration, // in-memory UTXO cache lookup (process_block pass 2)
    pass2_csv:    Duration, // pass 2: input/tx CSV writes (excl. lookup)
    utxo_delete:  Duration, // in-memory UTXO cache delete (process_block)
    commit:       Duration, // RocksDB WriteBatch + SQLite COMMIT + CSV flush
    checkpoint:   Duration, // UPDATE checkpoint row
    n_blocks:     u64,
    n_inputs:     u64,
    n_outputs:    u64,
}

impl Timings {
    fn print_report(&self, height: i64) {
        let total = self.read + self.decode + self.pass1_csv + self.utxo_insert
                  + self.utxo_lookup + self.pass2_csv + self.utxo_delete
                  + self.commit + self.checkpoint;
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        let pct = |d: Duration| if total.is_zero() { 0.0 } else { d.as_secs_f64() / total.as_secs_f64() * 100.0 };
        let n = self.n_blocks.max(1);
        println!(
            "  [perf @{height}] {:.0}ms/block over {} blocks  ({} inputs, {} outputs)",
            ms(total) / n as f64, n, self.n_inputs, self.n_outputs,
        );
        println!(
            "    read       {:>8.1}ms {:>5.1}%   decode     {:>8.1}ms {:>5.1}%",
            ms(self.read),   pct(self.read),
            ms(self.decode), pct(self.decode),
        );
        println!(
            "    pass1_csv  {:>8.1}ms {:>5.1}%   utxo_ins   {:>8.1}ms {:>5.1}%",
            ms(self.pass1_csv),   pct(self.pass1_csv),
            ms(self.utxo_insert), pct(self.utxo_insert),
        );
        println!(
            "    utxo_look  {:>8.1}ms {:>5.1}%   pass2_csv  {:>8.1}ms {:>5.1}%",
            ms(self.utxo_lookup), pct(self.utxo_lookup),
            ms(self.pass2_csv),   pct(self.pass2_csv),
        );
        println!(
            "    utxo_del   {:>8.1}ms {:>5.1}%   commit     {:>8.1}ms {:>5.1}%   checkpoint {:>8.1}ms {:>5.1}%",
            ms(self.utxo_delete), pct(self.utxo_delete),
            ms(self.commit),      pct(self.commit),
            ms(self.checkpoint),  pct(self.checkpoint),
        );
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

// ─── Phase 1: Block file indexing ─────────────────────────────────────────────

/// Scan all blk*.dat files and populate block_idx in SQLite.
/// Uses INSERT OR IGNORE so re-running is safe (idempotent).
fn index_block_files(files: &[PathBuf], db: &Connection) -> Result<()> {
    println!("Phase 1: indexing {} blk*.dat file(s)…", files.len());

    let mut insert = db.prepare(
        "INSERT OR IGNORE INTO block_idx (hash, prev_hash, file_path, byte_offset, block_size)
         VALUES (?1, ?2, ?3, ?4, ?5)"
    )?;

    for (file_no, path) in files.iter().enumerate() {
        let file_str = path.to_string_lossy().to_string();
        let mut f = BufReader::with_capacity(1 << 20, File::open(path)?);
        let mut blocks_in_file = 0u64;
        let mut file_offset: u64 = 0;

        loop {
            // Read magic (4 bytes)
            let mut magic = [0u8; 4];
            match f.read_exact(&mut magic) {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
                Ok(()) => {}
            }
            file_offset += 4;

            // Zero-padding at end of file → stop
            if magic == [0u8; 4] {
                break;
            }
            if magic != MAINNET_MAGIC {
                eprintln!("  WARN {}: unexpected magic {:?} at offset {}, skipping",
                          path.display(), magic, file_offset - 4);
                break;
            }

            // Read block size (4 bytes, little-endian)
            let mut size_buf = [0u8; 4];
            f.read_exact(&mut size_buf)?;
            file_offset += 4;
            let block_size = u32::from_le_bytes(size_buf) as u64;

            // byte_offset is where the raw block data starts (after magic+size)
            let block_data_offset = file_offset;

            // Parse just the 80-byte block header (fast path)
            let mut header_buf = [0u8; 80];
            f.read_exact(&mut header_buf)?;

            let header = BlockHeader::consensus_decode(&mut std::io::Cursor::new(&header_buf))
                .with_context(|| format!("{}: header parse failed at offset {}", path.display(), block_data_offset))?;

            let block_hash   = header.block_hash().to_string();
            let prev_hash    = header.prev_blockhash.to_string();

            insert.execute(params![block_hash, prev_hash, &file_str, block_data_offset as i64, block_size as i64])?;

            // Skip the rest of the block (we already read 80 bytes of it)
            let remaining = block_size.saturating_sub(80);
            io::copy(&mut f.by_ref().take(remaining), &mut io::sink())?;
            file_offset += block_size;
            blocks_in_file += 1;
        }

        println!("  [{}/{}] {} — {} blocks", file_no + 1, files.len(),
                 path.file_name().unwrap().to_string_lossy(), blocks_in_file);
    }

    // Assign heights by following the chain from genesis
    println!("  Assigning chain heights from genesis…");
    assign_heights(db)?;

    let count: i64 = db.query_row(
        "SELECT COUNT(*) FROM block_idx WHERE height IS NOT NULL", [], |r| r.get(0)
    )?;
    println!("  Main chain: {} blocks indexed.", count);
    Ok(())
}

/// BFS from genesis; assign height to each block on the main chain.
/// When a fork is encountered (orphan), prefer the branch with a child
/// (the orphan typically has no successor).
fn assign_heights(db: &Connection) -> Result<()> {
    // Reset any previously assigned heights
    db.execute("UPDATE block_idx SET height = NULL", [])?;

    let mut current_hash = GENESIS_HASH.to_string();
    let mut height: i64 = 0;

    let mut set_height = db.prepare(
        "UPDATE block_idx SET height = ?1 WHERE hash = ?2"
    )?;

    // Find next block: prefer one that has children (main chain over orphan)
    let mut next_stmt = db.prepare("
        SELECT bi.hash
        FROM block_idx bi
        WHERE bi.prev_hash = ?1
        ORDER BY (
            SELECT COUNT(*) FROM block_idx c WHERE c.prev_hash = bi.hash
        ) DESC
        LIMIT 1
    ")?;

    loop {
        set_height.execute(params![height, &current_hash])?;

        let next: Option<String> = next_stmt
            .query_row(params![&current_hash], |r| r.get(0))
            .optional()?;

        match next {
            Some(h) => {
                current_hash = h;
                height += 1;
            }
            None => break, // reached chain tip
        }
    }

    Ok(())
}

// ─── Phase 2: Block processing ────────────────────────────────────────────────

/// Read raw block bytes from a .blk file at the stored offset.
/// Takes a cache of open file handles to avoid reopening on every call.
fn read_raw_block(
    file_path: &str,
    byte_offset: i64,
    block_size: i64,
    file_cache: &mut FxHashMap<String, File>,
) -> Result<Vec<u8>> {
    let f = if let Some(f) = file_cache.get_mut(file_path) {
        f
    } else {
        let f = File::open(file_path)
            .with_context(|| format!("open {file_path}"))?;
        file_cache.insert(file_path.to_string(), f);
        file_cache.get_mut(file_path).unwrap()
    };
    f.seek(SeekFrom::Start(byte_offset as u64))?;
    let mut buf = vec![0u8; block_size as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

/// Process one block: write all CSV rows and update the UTXO cache.
/// Both passes are wrapped in a single SQLite transaction (BEGIN/COMMIT
/// is handled by the caller for batching across blocks).
fn process_block(
    block: &Block,
    height: i64,
    prev_block_hash: Option<&str>,
    w: &mut Writers,
    pending_inserts: &mut FxHashMap<([u8; 32], u32), (i64, Option<String>)>,
    pending_deletes: &mut FxHashSet<([u8; 32], u32)>,
    utxo_cache: &mut FxHashMap<([u8; 32], u32), (i64, Option<String>)>,
    t: &mut Timings,
) -> Result<()> {
    let block_hash = block.header.block_hash().to_string();
    let prev_hash  = block.header.prev_blockhash.to_string();
    let time_str   = fmt_datetime(block.header.time);
    let bits_u32   = block.header.bits.to_consensus();
    let bits_hex   = format!("{:08x}", bits_u32);
    let difficulty = bits_to_difficulty(bits_u32);
    let block_size = block.total_size() as i64;
    let block_weight = block.weight().to_wu() as i64;

    // Block node
    let merkle_root_str = block.header.merkle_root.to_string();
    let difficulty_str  = format!("{:.8}", difficulty);
    let (mut b1, mut b2, mut b3, mut b4, mut b5, mut b6) =
        (ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new());
    w.nodes_block.write_record(&[
        &block_hash, b1.format(height), &prev_hash, &merkle_root_str,
        &time_str, b2.format(block.txdata.len()), b3.format(block_size),
        b4.format(block_weight), &bits_hex, &difficulty_str,
        b5.format(block.header.nonce), b6.format(block.header.version.to_consensus()),
    ])?;

    // NEXT_BLOCK relationship (skip for genesis)
    if let Some(prev) = prev_block_hash {
        w.rels_next_block.write_record(&[prev, &block_hash])?;
    }

    // ─────────────────────────────────────────────────────────────────────
    // PASS 1: outputs — write Output/Address nodes, cache UTXOs in SQLite.
    //
    // All outputs are cached BEFORE Pass 2 so that inputs later in the
    // SAME block can look up UTXOs created by earlier transactions.
    // ─────────────────────────────────────────────────────────────────────

    // (txid, total_output) carried forward to Pass 2
    let mut tx_totals: Vec<(String, i64)> = Vec::with_capacity(block.txdata.len());

    let mut utxo_inserts: Vec<([u8; 32], u32, i64, Option<String>)> =
        Vec::with_capacity(block.txdata.len() * 2);

    // Reusable per-block buffers — avoids one heap allocation per output.
    let mut benefits: FxHashMap<String, (i32, i64)> = FxHashMap::default();
    let mut output_id    = String::with_capacity(80);  // txid(64) + ':' + vout
    let mut script_hex_buf: Vec<u8> = Vec::with_capacity(202); // 101-byte script * 2
    let mut ibuf_n   = ItoaBuf::new();
    let mut ibuf_amt = ItoaBuf::new();
    let mut ibuf_cnt = ItoaBuf::new();
    let mut ibuf_rcv = ItoaBuf::new();

    let t0 = Instant::now();
    for tx in &block.txdata {
        let txid_obj   = tx.compute_txid();
        let txid       = txid_obj.to_string();      // hex string – for CSV
        let txid_bytes = *txid_obj.as_byte_array(); // 32 raw bytes – for cache
        let mut total_output: i64 = 0;

        benefits.clear();

        for (n, txout) in tx.output.iter().enumerate() {
            let amount = txout.value.to_sat() as i64;
            let spk    = &txout.script_pubkey;
            let stype  = script_type(spk);
            let address = script_address(spk);
            total_output += amount;

            // Build output_id in place: txid + ':' + n
            output_id.clear();
            output_id.push_str(&txid);
            output_id.push(':');
            output_id.push_str(ibuf_n.format(n));

            // Hex-encode script into reusable buffer (no allocation)
            let spk_bytes = spk.as_bytes();
            script_hex_buf.resize(spk_bytes.len() * 2, 0);
            hex::encode_to_slice(spk_bytes, &mut script_hex_buf).unwrap();

            // Output node (isSpent=false; populated by post_import.cypher)
            w.nodes_output.write_record(&[
                output_id.as_bytes(), ibuf_n.format(n).as_bytes(),
                ibuf_amt.format(amount).as_bytes(),
                &script_hex_buf, stype.as_bytes(), b"false", b"", b"",
            ])?;
            w.rels_has_output.write_record(&[txid.as_bytes(), output_id.as_bytes()])?;

            if let Some(ref addr) = address {
                w.nodes_address.write_record(&[addr.as_bytes()])?;
                w.rels_locked_to.write_record(&[output_id.as_bytes(), addr.as_bytes()])?;
                let e = benefits.entry(addr.clone()).or_insert((0, 0));
                e.0 += 1;
                e.1 += amount;
            }

            utxo_inserts.push((txid_bytes, n as u32, amount, address));
            t.n_outputs += 1;
        }

        // BENEFITS_TO – one relationship per unique recipient address per tx
        for (addr, (cnt, received)) in &benefits {
            w.rels_benefits_to.write_record(&[
                txid.as_bytes(), addr.as_bytes(),
                ibuf_cnt.format(*cnt).as_bytes(), ibuf_rcv.format(*received).as_bytes(),
            ])?;
        }

        tx_totals.push((txid, total_output));
    }
    t.pass1_csv += t0.elapsed();

    // Stage outputs in the deferred pending_inserts map and the in-memory cache.
    // Actual SQLite writes are deferred to commit time so transient UTXOs
    // (created and spent within the same batch) never touch disk at all.
    let t0 = Instant::now();
    for (txid_bytes, vout, amount, addr) in &utxo_inserts {
        let key = (*txid_bytes, *vout);
        pending_inserts.insert(key, (*amount, addr.clone()));
        utxo_cache.insert(key, (*amount, addr.clone()));
    }
    t.utxo_insert += t0.elapsed();

    // PRE-PASS eliminated: UTXO lookups now served from the in-memory
    // utxo_cache (loaded at startup and kept in sync), so utxo_look ≈ 0.

    // ─────────────────────────────────────────────────────────────────────
    // PASS 2: inputs — use in-memory cache, compute fees, write Transaction
    //         nodes, Input nodes, and PERFORMS/SPENDS relationships.
    // ─────────────────────────────────────────────────────────────────────

    let mut utxo_deletes: Vec<([u8; 32], u32)> = Vec::new();

    let t0 = Instant::now();
    for (tx, (txid, total_output)) in block.txdata.iter().zip(tx_totals.iter()) {
        let is_coinbase = tx.is_coinbase();
        let mut total_input: i64 = 0;

        // address → (inputCount, amountSpent) for PERFORMS
        let mut performs: FxHashMap<String, (i32, i64)> = FxHashMap::default();

        let mut ibuf_idx = ItoaBuf::new();
        let mut ibuf_seq = ItoaBuf::new();
        let mut ibuf_pvout = ItoaBuf::new();
        for (idx, txin) in tx.input.iter().enumerate() {
            let input_id       = format!("{}:{}", txid, idx);
            let sequence       = txin.sequence.0 as i64;
            let witness        = encode_witness(&txin.witness);
            let script_sig_hex = hex::encode(txin.script_sig.as_bytes());

            w.nodes_input.write_record(&[
                &input_id, ibuf_idx.format(idx), &script_sig_hex,
                ibuf_seq.format(sequence), &witness,
            ])?;
            w.rels_has_input.write_record(&[&txid, &input_id])?;
            t.n_inputs += 1;

            if !is_coinbase {
                let prev_txid   = &txin.previous_output.txid; // &bitcoin::Txid
                let prev_vout   = txin.previous_output.vout;
                let prev_out_id = format!("{}:{}", prev_txid, ibuf_pvout.format(prev_vout));
                let prev_bytes  = *prev_txid.as_byte_array(); // [u8; 32], no alloc

                w.rels_spends.write_record(&[&input_id, &prev_out_id])?;

                match utxo_cache.get(&(prev_bytes, prev_vout)) {
                    Some((amt, addr)) => {
                        total_input += amt;
                        if let Some(a) = addr {
                            let e = performs.entry(a.clone()).or_insert((0, 0));
                            e.0 += 1;
                            e.1 += amt;
                        }
                    }
                    None => {
                        // Expected only when --start > 0 (missing genesis UTXO history)
                        eprintln!("  WARN block {height}: UTXO not found {}:{}", prev_txid, prev_vout);
                    }
                }

                utxo_deletes.push((prev_bytes, prev_vout));
            }
        }

        // Coinbase: totalInput = 0, fee = 0
        let (total_input, fee) = if is_coinbase {
            (0i64, 0i64)
        } else {
            (total_input, total_input - total_output)
        };

        // Transaction total size = serialized byte length (includes witness data).
        // vsize and weight are cheaper to compute from the parsed struct.
        let tx_size   = tx.total_size() as i64;
        let tx_vsize  = tx.vsize() as i64;
        let tx_weight = tx.weight().to_wu() as i64;

        // Transaction node
        let (mut tb1, mut tb2, mut tb3, mut tb4, mut tb5, mut tb6, mut tb7, mut tb8, mut tb9) =
            (ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new(),
             ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new(), ItoaBuf::new());
        let coinbase_str = if is_coinbase { "true" } else { "false" };
        w.nodes_transaction.write_record(&[
            txid.as_str(), tb1.format(height), block_hash.as_str(), time_str.as_str(),
            tb2.format(total_input), tb3.format(*total_output), tb4.format(fee),
            tb5.format(tx_size), tb6.format(tx_vsize), tb7.format(tx_weight),
            tb8.format(tx.version.0), tb9.format(tx.lock_time.to_consensus_u32()),
            coinbase_str,
        ])?;
        w.rels_included_in.write_record(&[&txid, &block_hash])?;

        // PERFORMS – one relationship per unique sender address per tx
        let mut ibuf_cnt = ItoaBuf::new();
        let mut ibuf_spt = ItoaBuf::new();
        for (addr, (cnt, spent)) in &performs {
            w.rels_performs.write_record(&[
                addr.as_str(), &txid, ibuf_cnt.format(*cnt), ibuf_spt.format(*spent),
            ])?;
        }
    }
    t.pass2_csv += t0.elapsed();

    // Remove spent UTXOs from the in-memory cache and from the deferred maps.
    // If the UTXO was inserted this batch (pending_inserts contains it), it's
    // transient and needs no SQLite work at all — just drop it from both maps.
    // Otherwise mark it for SQLite DELETE at commit time.
    let t0 = Instant::now();
    for (txid_bytes, vout) in utxo_deletes.drain(..) {
        let key = (txid_bytes, vout);
        if pending_inserts.remove(&key).is_none() {
            // Pre-existing UTXO (from a prior batch, already in SQLite)
            pending_deletes.insert(key);
        }
        utxo_cache.remove(&key);
    }
    t.utxo_delete += t0.elapsed();

    t.n_blocks += 1;
    Ok(())
}

// ─── Main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let args = Args::parse();

    let blocks_dir = expand_path(&args.blocks_dir);
    let output_dir = expand_path(&args.output_dir);
    fs::create_dir_all(&output_dir)?;

    let db_path      = output_dir.join("utxo_cache.db");
    let utxo_db_path = output_dir.join("utxo_cache.rocksdb");
    let db      = open_db(&db_path)?;
    let utxo_db = open_utxo_db(&utxo_db_path)?;

    // ── Phase 1: build block index ────────────────────────────────────────
    let phase: String = db.query_row(
        "SELECT phase FROM checkpoint WHERE id = 1", [], |r| r.get(0)
    )?;
    let need_index = args.reindex || phase == "index";

    if need_index {
        if args.reindex {
            db.execute("DELETE FROM block_idx", [])?;
            db.execute("UPDATE checkpoint SET phase='index', last_height=-1", [])?;
        }
        let files = blk_files(&blocks_dir)
            .with_context(|| format!("reading block files from {}", blocks_dir.display()))?;
        if files.is_empty() {
            bail!("No blk*.dat files found in {}", blocks_dir.display());
        }
        index_block_files(&files, &db)?;
        db.execute("UPDATE checkpoint SET phase='process' WHERE id=1", [])?;
        println!();
    } else {
        println!("Phase 1: block index already built (use --reindex to rebuild).");
    }

    // ── Phase 2: process blocks ───────────────────────────────────────────
    let last_height: i64 = db.query_row(
        "SELECT last_height FROM checkpoint WHERE id=1", [], |r| r.get(0)
    )?;

    let start = args.start.unwrap_or_else(|| {
        if last_height >= 0 {
            println!("Resuming from block {} (last checkpoint: {}).", last_height + 1, last_height);
            last_height + 1
        } else {
            0
        }
    });

    let chain_tip: i64 = db.query_row(
        "SELECT COALESCE(MAX(height), 0) FROM block_idx WHERE height IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    let end = args.end.unwrap_or(chain_tip);

    println!("Phase 2: processing blocks {}..={} (chain tip: {})", start, end, chain_tip);

    // Write header files (always regenerate)
    write_headers(&output_dir)?;

    // Open CSV writers. Append when resuming so we don't lose previous work.
    let appending = last_height >= 0 && args.start.is_none();
    let mut w = Writers::open(&output_dir, appending)?;

    const COMMIT_EVERY: i64 = 2000;
    const LOG_EVERY: i64    = 1000;

    let mut prev_block_hash: Option<String> = if start > 0 {
        db.query_row(
            "SELECT hash FROM block_idx WHERE height = ?1",
            params![start - 1],
            |r| r.get(0),
        ).optional()?
    } else {
        None
    };

    // Load the entire UTXO set from RocksDB into memory.
    // RocksDB is persistence-only; the in-memory FxHashMap is the live UTXO set.
    print!("  Loading UTXO cache from RocksDB... ");
    io::stdout().flush().ok();
    let mut utxo_cache: FxHashMap<([u8; 32], u32), (i64, Option<String>)> = FxHashMap::default();
    for item in utxo_db.iterator(rocksdb::IteratorMode::Start) {
        let (k, v) = item?;
        let txid: [u8; 32] = k[..32].try_into().unwrap();
        let vout = u32::from_be_bytes(k[32..36].try_into().unwrap());
        let (amount, addr) = utxo_val_decode(&v);
        utxo_cache.insert((txid, vout), (amount, addr));
    }
    println!("{} UTXOs loaded.", utxo_cache.len());

    let total = (end - start + 1).max(1);
    let mut timings = Timings::default();

    // Deferred UTXO maps: flushed to RocksDB via WriteBatch at commit time.
    // Transient UTXOs (inserted and spent within the same batch) cancel out
    // in process_block and never reach RocksDB, saving ~40% of write traffic.
    let batch_cap = COMMIT_EVERY as usize;
    let mut pending_inserts: FxHashMap<([u8; 32], u32), (i64, Option<String>)> =
        FxHashMap::with_capacity_and_hasher(batch_cap * 1500, Default::default());
    let mut pending_deletes: FxHashSet<([u8; 32], u32)> =
        FxHashSet::with_capacity_and_hasher(batch_cap * 1200, Default::default());

    // ── Reader thread: pre-fetch + decode blocks, overlapping disk I/O with
    //    the main thread's UTXO/CSV/RocksDB work. ─────────────────────────────
    //
    // Collect all block metadata upfront so the reader thread owns its data
    // without holding a SQLite statement open across the channel lifetime.
    let mut block_metas: Vec<(i64, String, i64, i64, String)> =
        Vec::with_capacity(total as usize);
    {
        let mut stmt = db.prepare(
            "SELECT height, file_path, byte_offset, block_size, hash
             FROM block_idx WHERE height >= ?1 AND height <= ?2 ORDER BY height"
        )?;
        let mut rows = stmt.query(params![start, end])?;
        while let Some(row) = rows.next()? {
            block_metas.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?));
        }
    }

    // Bound = 8 blocks: enough pipeline depth without unbounded memory growth
    // (post-2016 blocks can be ~1 MB each → ≤8 MB buffered).
    let (tx, rx) = mpsc::sync_channel::<anyhow::Result<BlockMsg>>(8);
    thread::spawn(move || {
        let mut file_cache: FxHashMap<String, File> = FxHashMap::default();
        for (height, file_path, byte_offset, block_size, hash) in block_metas {
            let t_read = Instant::now();
            let result = read_raw_block(&file_path, byte_offset, block_size, &mut file_cache)
                .with_context(|| format!("reading block {height}"));
            let read_us = t_read.elapsed().as_micros() as u64;
            let msg = result.and_then(|raw| {
                let t_dec = Instant::now();
                let block = Block::consensus_decode(&mut std::io::Cursor::new(&raw))
                    .with_context(|| format!("decoding block {height}"))?;
                Ok(BlockMsg { height, block, hash, read_us, decode_us: t_dec.elapsed().as_micros() as u64 })
            });
            if tx.send(msg).is_err() { break; }
        }
    });

    db.execute("BEGIN", [])?;

    for msg in rx {
        let BlockMsg { height, block, hash, read_us, decode_us } = msg?;
        timings.read   += Duration::from_micros(read_us);
        timings.decode += Duration::from_micros(decode_us);

        process_block(&block, height, prev_block_hash.as_deref(), &mut w,
                      &mut pending_inserts, &mut pending_deletes, &mut utxo_cache, &mut timings)?;

        let t0 = Instant::now();
        db.execute("UPDATE checkpoint SET last_height=?1 WHERE id=1", params![height])?;
        timings.checkpoint += t0.elapsed();

        prev_block_hash = Some(hash);

        if height % COMMIT_EVERY == 0 || height == end {
            let t0 = Instant::now();

            // Build RocksDB WriteBatch: puts for new UTXOs, deletes (LSM
            // tombstones) for spent ones. Sort by key for better compaction locality.
            let mut inserts: Vec<_> = pending_inserts.drain().collect();
            inserts.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let mut deletes: Vec<_> = pending_deletes.drain().collect();
            deletes.sort_unstable();

            let t_rdb0 = Instant::now();
            let mut batch = WriteBatch::default();
            for ((txid, vout), (amount, addr)) in &inserts {
                batch.put(utxo_key(txid, *vout), utxo_val_encode(*amount, addr));
            }
            for (txid, vout) in &deletes {
                batch.delete(utxo_key(txid, *vout));
            }
            utxo_db.write(batch)?;
            let t_rdb_ms = t_rdb0.elapsed().as_secs_f64() * 1000.0;

            let t_commit0 = Instant::now();
            db.execute("COMMIT", [])?;
            let t_commit_ms = t_commit0.elapsed().as_secs_f64() * 1000.0;

            let t_ckpt0 = Instant::now();
            db.execute_batch("PRAGMA wal_checkpoint(RESTART)")?;
            let t_ckpt_ms = t_ckpt0.elapsed().as_secs_f64() * 1000.0;

            w.flush_all()?;
            timings.commit += t0.elapsed();
            println!("    [commit @{height}] rdb={t_rdb_ms:.0}ms commit={t_commit_ms:.0}ms ckpt={t_ckpt_ms:.0}ms");
            if height < end {
                db.execute("BEGIN", [])?;
            }
        }

        if height % LOG_EVERY == 0 || height == end {
            let done = (height - start + 1) as f64 / total as f64 * 100.0;
            println!("  block {:>9} / {}  ({:.1}%)", height, end, done);
            timings.print_report(height);
            timings.reset();
        }
    }

    println!("\nDone. CSV files written to {}/", output_dir.display());
    println!("Next steps:");
    println!("  1. Run ../import.sh {} to load into Neo4j", output_dir.display());
    println!("  2. Run ../post_import.cypher to populate Output.isSpent fields");

    Ok(())
}
