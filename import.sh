#!/usr/bin/env bash
# Import Bitcoin blockchain CSV files into Neo4j.
# Run AFTER btc_to_csv.py has finished exporting.
#
# Usage:
#   ./import.sh [database-name]
#
# The default database name is "neo4j". Change DB below if needed.
# Requires neo4j-admin to be on your PATH (or provide full path).

set -euo pipefail

CSV_DIR="${1:-./bitcoin_csv}"
DB="${2:-neo4j}"

echo "Importing CSV files from: $CSV_DIR"
echo "Target database:          $DB"
echo ""

neo4j-admin database import full \
  --nodes=Block="$CSV_DIR/nodes_block-header.csv,$CSV_DIR/nodes_block.csv" \
  --nodes=Transaction="$CSV_DIR/nodes_transaction-header.csv,$CSV_DIR/nodes_transaction.csv" \
  --nodes=Output="$CSV_DIR/nodes_output-header.csv,$CSV_DIR/nodes_output.csv" \
  --nodes=Input="$CSV_DIR/nodes_input-header.csv,$CSV_DIR/nodes_input.csv" \
  --nodes=Address="$CSV_DIR/nodes_address-header.csv,$CSV_DIR/nodes_address.csv" \
  --relationships=NEXT_BLOCK="$CSV_DIR/rels_next_block-header.csv,$CSV_DIR/rels_next_block.csv" \
  --relationships=INCLUDED_IN="$CSV_DIR/rels_included_in-header.csv,$CSV_DIR/rels_included_in.csv" \
  --relationships=HAS_INPUT="$CSV_DIR/rels_has_input-header.csv,$CSV_DIR/rels_has_input.csv" \
  --relationships=HAS_OUTPUT="$CSV_DIR/rels_has_output-header.csv,$CSV_DIR/rels_has_output.csv" \
  --relationships=SPENDS="$CSV_DIR/rels_spends-header.csv,$CSV_DIR/rels_spends.csv" \
  --relationships=LOCKED_TO="$CSV_DIR/rels_locked_to-header.csv,$CSV_DIR/rels_locked_to.csv" \
  --relationships=PERFORMS="$CSV_DIR/rels_performs-header.csv,$CSV_DIR/rels_performs.csv" \
  --relationships=BENEFITS_TO="$CSV_DIR/rels_benefits_to-header.csv,$CSV_DIR/rels_benefits_to.csv" \
  --ignore-duplicate-nodes=true \
  --id-type=STRING \
  --overwrite-destination=true \
  "$DB"

echo ""
echo "Import complete."
echo "After starting Neo4j, run post_import.cypher to populate Output.isSpent fields."
