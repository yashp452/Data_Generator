#!/bin/bash

CONFIG="configs/retail.yaml"
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/datagen_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

echo "========================================" | tee -a "$LOG_FILE"
echo "Run started at: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

python3 main.py --config "$CONFIG" 2>&1 | tee -a "$LOG_FILE"

if [ $? -eq 0 ]; then
    echo "========================================" | tee -a "$LOG_FILE"
    echo "Run completed successfully at: $(date)" | tee -a "$LOG_FILE"
else
    echo "========================================" | tee -a "$LOG_FILE"
    echo "Run FAILED at: $(date)" | tee -a "$LOG_FILE"
    exit 1
fi
