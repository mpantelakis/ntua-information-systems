#!/bin/bash

# Check if path is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <path-to-generated-data>"
    exit 1
fi

DATA_DIR=$1

# Go to the data directory
cd "$DATA_DIR" || { echo "Directory not found: $DATA_DIR"; exit 1; }

#Remove trailing commas from all .dat files
for f in *.dat; do
    echo "Cleaning $f..."
    sed -i 's/|$//' "$f"
done

echo "All files cleaned."