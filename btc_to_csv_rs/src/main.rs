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

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use bitcoin::{
    block::Header as BlockHeader,
    consensus::Decodable,
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
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-524288; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=17179869184;")?;
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

        CREATE TABLE IF NOT EXISTS utxo (
            txid    TEXT    NOT NULL,
            vout    INTEGER NOT NULL,
            amount  INTEGER NOT NULL,
            address TEXT,
            PRIMARY KEY (txid, vout)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS checkpoint (
            id           INTEGER PRIMARY KEY CHECK (id = 1),
            phase        TEXT    NOT NULL DEFAULT 'index',
            last_height  INTEGER NOT NULL DEFAULT -1
        );
        INSERT OR IGNORE INTO checkpoint (id, phase, last_height) VALUES (1, 'index', -1);
    ")?;
    Ok(conn)
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
    let buf = BufWriter::with_capacity(1 << 20, file); // 1 MB write buffer
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
    read:         Duration, // read_raw_block (file I/O)
    decode:       Duration, // consensus_decode
    pass1_csv:    Duration, // pass 1: output/address CSV writes
    utxo_insert:  Duration, // batch UTXO inserts to SQLite
    utxo_lookup:  Duration, // per-input UTXO SELECT in pass 2
    pass2_csv:    Duration, // pass 2: input/tx CSV writes (excl. lookup)
    utxo_delete:  Duration, // batch UTXO deletes from SQLite
    commit:       Duration, // SQLite COMMIT + CSV flush
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
    file_cache: &mut HashMap<String, File>,
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
    db: &Connection,
    utxo_cache: &mut HashMap<(String, u32), (i64, Option<String>)>,
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

    // Block node — all fields as String to keep write_record type consistent
    w.nodes_block.write_record(vec![
        block_hash.clone(),
        height.to_string(),
        prev_hash.clone(),
        block.header.merkle_root.to_string(),
        time_str.clone(),
        block.txdata.len().to_string(),
        block_size.to_string(),
        block_weight.to_string(),
        bits_hex,
        format!("{:.8}", difficulty),
        block.header.nonce.to_string(),
        block.header.version.to_consensus().to_string(),
    ])?;

    // NEXT_BLOCK relationship (skip for genesis)
    if let Some(prev) = prev_block_hash {
        w.rels_next_block.write_record(vec![prev.to_string(), block_hash.clone()])?;
    }

    // ─────────────────────────────────────────────────────────────────────
    // PASS 1: outputs — write Output/Address nodes, cache UTXOs in SQLite.
    //
    // All outputs are cached BEFORE Pass 2 so that inputs later in the
    // SAME block can look up UTXOs created by earlier transactions.
    // ─────────────────────────────────────────────────────────────────────

    // (txid, total_output) carried forward to Pass 2
    let mut tx_totals: Vec<(String, i64)> = Vec::with_capacity(block.txdata.len());

    let mut utxo_inserts: Vec<(String, u32, i64, Option<String>)> =
        Vec::with_capacity(block.txdata.len() * 2);

    let t0 = Instant::now();
    for tx in &block.txdata {
        let txid = tx.compute_txid().to_string();
        let mut total_output: i64 = 0;

        // address → (outputCount, amountReceived) for BENEFITS_TO
        let mut benefits: HashMap<String, (i32, i64)> = HashMap::new();

        for (n, txout) in tx.output.iter().enumerate() {
            let output_id   = format!("{}:{}", txid, n);
            let amount      = txout.value.to_sat() as i64;
            let spk         = &txout.script_pubkey;
            let script_hex  = hex::encode(spk.as_bytes());
            let stype       = script_type(spk).to_string();
            let address     = script_address(spk);
            total_output   += amount;

            // Output node (isSpent=false; populated by post_import.cypher)
            w.nodes_output.write_record(vec![
                output_id.clone(),
                n.to_string(),
                amount.to_string(),
                script_hex,
                stype,
                "false".to_string(), String::new(), String::new(),
            ])?;
            w.rels_has_output.write_record(vec![txid.clone(), output_id.clone()])?;

            if let Some(ref addr) = address {
                w.nodes_address.write_record(vec![addr.clone()])?;
                w.rels_locked_to.write_record(vec![output_id.clone(), addr.clone()])?;
                let e = benefits.entry(addr.clone()).or_insert((0, 0));
                e.0 += 1;
                e.1 += amount;
            }

            utxo_inserts.push((txid.clone(), n as u32, amount, address));
            t.n_outputs += 1;
        }

        // BENEFITS_TO – one relationship per unique recipient address per tx
        for (addr, (cnt, received)) in &benefits {
            w.rels_benefits_to.write_record(vec![
                txid.clone(), addr.clone(), cnt.to_string(), received.to_string(),
            ])?;
        }

        tx_totals.push((txid, total_output));
    }
    t.pass1_csv += t0.elapsed();

    // Insert outputs into both SQLite (for persistence/resume) and the in-memory
    // cache (for O(1) lookup in pass 2 without any SQLite round-trips).
    let t0 = Instant::now();
    {
        let mut ins = db.prepare_cached(
            "INSERT OR REPLACE INTO utxo (txid, vout, amount, address) VALUES (?1,?2,?3,?4)"
        )?;
        for (txid, vout, amount, addr) in &utxo_inserts {
            ins.execute(params![txid, *vout, *amount, addr])?;
            utxo_cache.insert((txid.clone(), *vout), (*amount, addr.clone()));
        }
    }
    t.utxo_insert += t0.elapsed();

    // PRE-PASS eliminated: UTXO lookups now served from the in-memory
    // utxo_cache (loaded at startup and kept in sync), so utxo_look ≈ 0.

    // ─────────────────────────────────────────────────────────────────────
    // PASS 2: inputs — use in-memory cache, compute fees, write Transaction
    //         nodes, Input nodes, and PERFORMS/SPENDS relationships.
    // ─────────────────────────────────────────────────────────────────────

    let mut utxo_deletes: Vec<(String, u32)> = Vec::new();

    let t0 = Instant::now();
    for (tx, (txid, total_output)) in block.txdata.iter().zip(tx_totals.iter()) {
        let is_coinbase = tx.is_coinbase();
        let mut total_input: i64 = 0;

        // address → (inputCount, amountSpent) for PERFORMS
        let mut performs: HashMap<String, (i32, i64)> = HashMap::new();

        for (idx, txin) in tx.input.iter().enumerate() {
            let input_id       = format!("{}:{}", txid, idx);
            let sequence       = txin.sequence.0 as i64;
            let witness        = encode_witness(&txin.witness);
            let script_sig_hex = hex::encode(txin.script_sig.as_bytes());

            w.nodes_input.write_record(vec![
                input_id.clone(),
                idx.to_string(),
                script_sig_hex,
                sequence.to_string(),
                witness,
            ])?;
            w.rels_has_input.write_record(vec![txid.clone(), input_id.clone()])?;
            t.n_inputs += 1;

            if !is_coinbase {
                let prev_txid   = txin.previous_output.txid.to_string();
                let prev_vout   = txin.previous_output.vout;
                let prev_out_id = format!("{}:{}", prev_txid, prev_vout);

                w.rels_spends.write_record(vec![input_id, prev_out_id])?;

                match utxo_cache.get(&(prev_txid.clone(), prev_vout)) {
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
                        eprintln!("  WARN block {height}: UTXO not found {prev_txid}:{prev_vout}");
                    }
                }

                utxo_deletes.push((prev_txid, prev_vout));
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
        w.nodes_transaction.write_record(vec![
            txid.clone(),
            height.to_string(),
            block_hash.clone(),
            time_str.clone(),
            total_input.to_string(),
            total_output.to_string(),
            fee.to_string(),
            tx_size.to_string(),
            tx_vsize.to_string(),
            tx_weight.to_string(),
            tx.version.0.to_string(),
            tx.lock_time.to_consensus_u32().to_string(),
            (if is_coinbase { "true" } else { "false" }).to_string(),
        ])?;
        w.rels_included_in.write_record(vec![txid.clone(), block_hash.clone()])?;

        // PERFORMS – one relationship per unique sender address per tx
        for (addr, (cnt, spent)) in &performs {
            w.rels_performs.write_record(vec![
                addr.clone(), txid.clone(), cnt.to_string(), spent.to_string(),
            ])?;
        }
    }
    t.pass2_csv += t0.elapsed();

    // Remove spent UTXOs from both SQLite and the in-memory cache.
    // Drain utxo_deletes so we can move each txid into the HashMap key
    // without a clone (SQLite borrows it first, then it moves into remove).
    let t0 = Instant::now();
    {
        let mut del = db.prepare_cached("DELETE FROM utxo WHERE txid=?1 AND vout=?2")?;
        for (txid, vout) in utxo_deletes.drain(..) {
            del.execute(params![&txid, vout])?;
            utxo_cache.remove(&(txid, vout));
        }
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

    let db_path = output_dir.join("utxo_cache.db");
    let db = open_db(&db_path)?;

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

    // Prepared statement for block lookup
    let mut block_lookup = db.prepare(
        "SELECT file_path, byte_offset, block_size, hash
         FROM block_idx WHERE height = ?1"
    )?;

    const COMMIT_EVERY: i64 = 2000; // SQLite commit + CSV flush interval (blocks)
    const LOG_EVERY: i64    = 1000;

    db.execute("BEGIN", [])?;
    let mut prev_block_hash: Option<String> = if start > 0 {
        db.query_row(
            "SELECT hash FROM block_idx WHERE height = ?1",
            params![start - 1],
            |r| r.get(0),
        ).optional()?
    } else {
        None
    };

    // Load the entire UTXO set into memory so pass 2 can do O(1) lookups
    // without any SQLite round-trips. SQLite remains the source of truth for
    // persistence; the cache is just a mirror kept in sync during processing.
    print!("  Loading UTXO cache from SQLite... ");
    let mut utxo_cache: HashMap<(String, u32), (i64, Option<String>)> = HashMap::new();
    {
        let mut stmt = db.prepare("SELECT txid, vout, amount, address FROM utxo")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            utxo_cache.insert(
                (row.get(0)?, row.get::<_, u32>(1)?),
                (row.get(2)?, row.get(3)?),
            );
        }
    }
    println!("{} UTXOs loaded.", utxo_cache.len());

    let total = (end - start + 1).max(1);
    let mut file_cache: HashMap<String, File> = HashMap::new();
    let mut timings = Timings::default();

    for height in start..=end {
        let (file_path, byte_offset, block_size, hash): (String, i64, i64, String) =
            block_lookup.query_row(params![height], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            }).with_context(|| format!("block at height {height} not found in index"))?;

        let t0 = Instant::now();
        let raw = read_raw_block(&file_path, byte_offset, block_size, &mut file_cache)
            .with_context(|| format!("reading block {height} from {file_path}"))?;
        timings.read += t0.elapsed();

        let t0 = Instant::now();
        let block = Block::consensus_decode(&mut std::io::Cursor::new(&raw))
            .with_context(|| format!("decoding block {height}"))?;
        timings.decode += t0.elapsed();

        process_block(&block, height, prev_block_hash.as_deref(), &mut w, &db, &mut utxo_cache, &mut timings)?;

        let t0 = Instant::now();
        db.execute("UPDATE checkpoint SET last_height=?1 WHERE id=1", params![height])?;
        timings.checkpoint += t0.elapsed();

        prev_block_hash = Some(hash);

        if height % COMMIT_EVERY == 0 || height == end {
            let t0 = Instant::now();
            db.execute("COMMIT", [])?;
            // Reset WAL to zero after each commit so it never grows large.
            // Without this the WAL accumulates across commits and checkpoint
            // overhead compounds as the UTXO set grows.
            db.execute_batch("PRAGMA wal_checkpoint(RESTART)")?;
            w.flush_all()?;
            timings.commit += t0.elapsed();
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
