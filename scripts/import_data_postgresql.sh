#!/bin/bash

DB_NAME="tpcds"
DB_USER="postgres"
FIELDS_DIR="/home/user/fields-files"
TABLES_DIR="/home/user/tpcds-tables-postgresql"
DATA_DIR="/mnt/data/tpcds-data"
DELIM="|"

# List of tables to process (comment out any table you want to skip)
TABLES=(
    #store_sales
    #catalog_sales
    #web_sales
    #store_returns
    #catalog_returns
    #web_returns
    #inventory
    #item
    #date_dim
    #store
    #promotion
    #catalog_page
    #customer
    #customer_address
    #customer_demographics
    #household_demographics
    #income_band
    #warehouse
    #web_page
    #web_site
    #reason
    #ship_mode
    #call_center
    time_dim
)

#  Create database if it doesn't exist
DB_EXISTS=$(sudo -u "$DB_USER" psql -tc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'")
if [[ "$DB_EXISTS" != "1" ]]; then
    echo "Creating database $DB_NAME..."
    sudo -u "$DB_USER" psql -c "CREATE DATABASE $DB_NAME TABLESPACE fastspace;"
else
    echo "Database $DB_NAME already exists."
fi

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

# Create tables and import data
for TABLE_NAME in "${TABLES[@]}"; do
    SQL_FILE="$TABLES_DIR/$TABLE_NAME.sql"
    DATA_FILE="$DATA_DIR/$TABLE_NAME.dat"
    FIELD_FILE="$FIELDS_DIR/$TABLE_NAME"

    # Create table
    if [[ -f "$SQL_FILE" ]]; then
        echo "Creating table $TABLE_NAME..."
        sudo -u "$DB_USER" psql -d "$DB_NAME" -f "$SQL_FILE"
    else
        echo "Table definition file $SQL_FILE not found, skipping..."
        continue
    fi

    # Import data
    if [[ -f "$DATA_FILE" ]]; then
        echo "Importing data for $TABLE_NAME..."
        sudo -u "$DB_USER" psql -d "$DB_NAME" -c "\COPY $TABLE_NAME FROM '$DATA_FILE' WITH DELIMITER '$DELIM' CSV"
    else
        echo "Data file $DATA_FILE not found, skipping..."
    fi
done

echo "All done!"
