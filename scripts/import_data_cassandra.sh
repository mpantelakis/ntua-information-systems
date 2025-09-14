#!/bin/bash

KEYSPACE="tpcds"
SCHEMA_DIR="/home/user/tpcds-tables"
DATA_DIR="/mnt/data/tpcds-data"
DELIM="|"


# Ensure keyspace exists
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"


# Customer file encoding check
CUSTOMER_FILE="$DATA_DIR/customer.dat"
if [[ -f "$CUSTOMER_FILE" ]]; then
    ENCODING=$(file -i "$CUSTOMER_FILE" | awk -F "=" '{print $2}')
    if [[ "$ENCODING" == "iso-8859-1" ]]; then
        echo "Converting $CUSTOMER_FILE from ISO-8859-1 to UTF-8..."
        iconv -f ISO-8859-1 -t UTF-8 "$CUSTOMER_FILE" > "${CUSTOMER_FILE}_utf8"
        rm "$CUSTOMER_FILE"
        mv "${CUSTOMER_FILE}_utf8" "$CUSTOMER_FILE"
        echo "Conversion done."
    fi
else
    echo "Customer data file not found: $CUSTOMER_FILE"
fi

# All table names from tpcds-tables
TABLES=(
  # Fact tables
   #store_sales
   #store_returns
   #catalog_sales
   #catalog_returns
   #web_sales
   #web_returns
   #inventory

  # Dimension tables
   #store
   #call_center
   #catalog_page
   #web_site
   #web_page
   warehouse
   #customer
   #customer_address
   #customer_demographics
   date_dim
   #household_demographics
   item
   #income_band
   #promotion
   #reason
   #ship_mode
   #time_dim
)


for tbl in "${TABLES[@]}"; do
    CQL_FILE="$SCHEMA_DIR/$tbl.cql"
    DATA_FILE="$DATA_DIR/$tbl.dat"

    if [[ ! -f "$CQL_FILE" ]]; then
        echo "Schema file not found for $tbl ($CQL_FILE), skipping..."
    else
        echo -e "\nCreating table: $tbl"
        cqlsh -f "$CQL_FILE"
    fi

    if [[ ! -f "$DATA_FILE" ]]; then
        echo "Data file not found for $tbl ($DATA_FILE), skipping..."
        continue
    fi

    # Extract column names in order from CQL file, excluding PRIMARY KEY
    COLS=$(grep -E "^\s+[a-zA-Z0-9_]+ " "$CQL_FILE" | grep -vi "primary key" | \
       awk '{print $1}' | tr '\n' ',' | sed 's/,$//')


    echo -e "\nImporting $tbl with columns: $COLS"

    cqlsh -e "COPY ${KEYSPACE}.${tbl} (${COLS}) FROM '${DATA_FILE}' WITH DELIMITER='${DELIM}' AND HEADER=FALSE AND NULL='';"
done
