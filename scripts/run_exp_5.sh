#!/bin/bash

# This script runs experiment 5 for Analysis & Design of Information Systems 2024-2025.

QUERIES_DIR=~/tpc-ds/tpcds-queries/distribution-method-4
COORDINATOR_IP=<your-coordinator-ip>

echo "==========================================================="
echo "Commencing Experiment 5 using Data Distribution Strategy 4"
echo "==========================================================="
echo
echo


# Define the group of queries to be executed
queries=(
    "query_01 query_02 query_07 query_14 query_15 query_16 query_20 query_23 query_32"
    "query_05 query_08 query_12 query_13 query_17 query_21 query_22 query_24 query_39 query_94"
    "query_03 query_04 query_06 query_09 query_10 query_30 query_37 query_48 query_49"
)

# Define the output CSV file
OUTPUT_CSV="dist_4_results.csv"
# Write the header to the CSV file
header=""
for i in "${!queries[@]}"; do
    group="${queries[$i]}"
    for query in $group; do
        header="$header,$query"
    done
    header="$header,group_$((i+1))_total"
done
echo "$header" > "$OUTPUT_CSV"


# Repeat the entire set of queries multiple times
NUM_OF_ITERATIONS=5
csv_times=()
for run in $(seq 1 $NUM_OF_ITERATIONS); do
    echo "---------------------------------"
    echo "Iteration $run of the experiment"
    echo "---------------------------------"
    echo

    # Initialize a new line for the CSV output
    csv_line="iteration_$run"

    # Iterate over each group and execute the queries
    for i in "${!queries[@]}"; do
        group="${queries[$i]}"
        echo "Executing group $((i+1))"
        echo "--------------------------"

        # Initialize total runtime for the group
        group_runtime=0.00

        # Iterate over each query in the group
        for query in $group; do
            # Query file path to be executed
            QUERY_FILE=$QUERIES_DIR/${query}.sql

            # Run Presto CLI with the specified server, catalog, schema, and query file
            OUTPUT=$(
            ./presto_cli \
                --server http://$COORDINATOR_IP:8080 \
                < "$QUERY_FILE" 2>&1
            )

            # Extract the Query ID from the output
            QUERY_ID=$(printf '%s\n' "$OUTPUT" \
            | awk '/^Query /{gsub(",", "", $2); print $2; exit}')

            # Fetch runtime (seconds) and state
            read RUNTIME_SEC STATE < <(
                ./presto_cli \
                    --server http://$COORDINATOR_IP:8080 \
                    --catalog system \
                    --schema runtime \
                    --output-format TSV \
                    --execute "
                        SELECT round(date_diff('millisecond', created, \"end\") / 1000.00, 2) AS duration_sec,
                            state
                        FROM system.runtime.queries
                        WHERE query_id = '$QUERY_ID';
                    " | tail -n 1
            )

            # Accumulate total runtime for the group
            group_runtime=$(echo "$group_runtime + $RUNTIME_SEC" | bc)

            echo "${query} with id $QUERY_ID $STATE in $RUNTIME_SEC sec"

            # Append the runtime to the CSV line
            csv_line="$csv_line,$RUNTIME_SEC"
            csv_times+=("$RUNTIME_SEC")

            sleep 10
        done

        echo
        echo "Total runtime for group $((i+1)): $group_runtime sec"
        echo
        echo

        # Append the group total runtime to the CSV line
        csv_line="$csv_line,$group_runtime"
        csv_times+=("$group_runtime")
    done
    # Write the CSV line to the output file
    echo "$csv_line" >> "$OUTPUT_CSV"
done

# Calculate and display averages
avg_line="average"
avg_arr=()
NUM_OF_COLUMNS=$(( ${#csv_times[@]} / NUM_OF_ITERATIONS ))
for col in $(seq 0 $((NUM_OF_COLUMNS - 1))); do
    sum=0.00
    for row in $(seq 0 $((NUM_OF_ITERATIONS - 1))); do
        index=$(( row * NUM_OF_COLUMNS + col ))
        sum=$(echo "$sum + ${csv_times[$index]}" | bc)
    done
    avg=$(echo "scale=2; $sum / $NUM_OF_ITERATIONS" | bc)
    avg_line="$avg_line,$avg"
    avg_arr+=("$avg")
done

echo "$avg_line" >> "$OUTPUT_CSV"

echo "============================================"
index=0  # tracks position in avg_arr
for i in "${!queries[@]}"; do
    # Convert space-separated string into array to get size
    read -r -a group_cols <<< "${queries[$i]}"
    group_size=${#group_cols[@]}
    
    # The "group total" column is right after the group columns
    total_index=$(( index + group_size ))
    
    # Print group total average
    echo "Group $((i+1)) Total Average: ${avg_arr[$total_index]} seconds"
    echo "============================================"
    
    # Move index past this group and its total column
    index=$(( total_index + 1 ))
done


