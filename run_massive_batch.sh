#!/bin/bash
set -e

echo "Starting Massive Batch Generation (Parquet Format)..."

# 30 iterations = ~15 Million Orders & ~45 Million Line Items
TOTAL_RUNS=30

for ((i=1; i<=TOTAL_RUNS; i++)); do
  echo "═════════════════════════════════════════"
  echo "        BATCH ITERATION $i OF $TOTAL_RUNS"
  echo "═════════════════════════════════════════"
  python3 main.py --config configs/retail.yaml
done

echo "✅ Generation Complete! Check the ./output/retail/facts/ directory for the Parquet files."
