/// strip_witness: remove the last field (witness) from nodes_input.csv
/// Usage: strip_witness <input.csv> <output.csv>
///        or pipe: strip_witness < nodes_input.csv > nodes_input_fixed.csv
///
/// Reads line-by-line, finds the last unquoted comma, truncates there.
/// No CSV library needed — witness is always the last field.

use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};

fn strip_last_field(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_quotes = false;
    let mut last_comma = None;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'"' => in_quotes = !in_quotes,
            b',' if !in_quotes => last_comma = Some(i),
            _ => {}
        }
    }
    match last_comma {
        Some(pos) => &line[..pos],
        None => line,
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    let reader: Box<dyn BufRead> = if args.len() > 1 {
        Box::new(BufReader::with_capacity(1 << 20, File::open(&args[1])?))
    } else {
        Box::new(BufReader::with_capacity(1 << 20, io::stdin()))
    };

    let writer: Box<dyn Write> = if args.len() > 2 {
        Box::new(BufWriter::with_capacity(1 << 20, File::create(&args[2])?))
    } else {
        Box::new(BufWriter::with_capacity(1 << 20, io::stdout()))
    };

    let mut out = writer;

    for line in reader.lines() {
        let line = line?;
        let stripped = strip_last_field(&line);
        out.write_all(stripped.as_bytes())?;
        out.write_all(b"\n")?;
    }

    Ok(())
}
