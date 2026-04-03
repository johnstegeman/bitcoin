// Post-import: populate Output.isSpent, spentInTxid, spentAtHeight
//
// All outputs are imported with isSpent=false. The SPENDS relationships
// already encode which outputs are spent and by which inputs. This query
// traverses those relationships to populate the denormalised properties.
//
// Run once after neo4j-admin import completes and Neo4j is started.
// Requires APOC plugin for batched iteration on large datasets.
//
// Usage (cypher-shell):
//   cat post_import.cypher | cypher-shell -u neo4j -p <password>

// Create index on Output.outputId to speed up the SPENDS traversal
// (neo4j-admin import creates :ID constraints automatically, so these
//  may already exist – adjust if needed)
CREATE INDEX output_isSpent IF NOT EXISTS FOR (o:Output) ON (o.isSpent);

// Populate isSpent / spentInTxid / spentAtHeight via the SPENDS relationship.
// Uses apoc.periodic.iterate for memory-efficient batched processing.
CALL apoc.periodic.iterate(
  "MATCH (tx:Transaction)-[:HAS_INPUT]->(i:Input)-[:SPENDS]->(o:Output)
   RETURN tx.txid AS spentInTxid, tx.blockHeight AS spentAtHeight, o",
  "SET o.isSpent = true,
       o.spentInTxid = spentInTxid,
       o.spentAtHeight = spentAtHeight",
  {batchSize: 50000, parallel: false}
)
YIELD batches, total, timeTaken, errorMessages
RETURN batches, total, timeTaken, errorMessages;
