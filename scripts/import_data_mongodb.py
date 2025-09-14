import csv
from pymongo import MongoClient
import os
import subprocess

# Config
TYPEMAPS_DIR = "type_maps"
DATA_DIR = "/mnt/data/tpcds-data"    # directory with .dat files
DB_NAME = "tpcds"
MONGO_URI = "mongodb://localhost:27017/"

# === Special handling for customer table at the start ===
customer_file = os.path.join(DATA_DIR, "customer.dat")
if os.path.isfile(customer_file):
    # Detect encoding using `file -i`
    result = subprocess.run(["file", "-i", customer_file], capture_output=True, text=True)
    encoding = result.stdout.strip().split("=")[-1]
    if encoding.lower() == "iso-8859-1":
        print(f"Converting {customer_file} from ISO-8859-1 to UTF-8...")
        tmp_file = f"{customer_file}_utf8"
        with open(customer_file, "rb") as f_in, open(tmp_file, "wb") as f_out:
            f_out.write(f_in.read().decode("iso-8859-1").encode("utf-8"))
        os.remove(customer_file)
        os.rename(tmp_file, customer_file)
        print("Conversion done.")

# Full list of TPC-DS tables
TABLES = [
    # Fact tables
    #"store_sales",
    #"store_returns",
    #"catalog_sales",
    #"catalog_returns",
    "web_sales",
    "web_returns",
    #"inventory",

    # Dimension tables
    #"store",
    #"call_center",
    #"catalog_page",
    "web_site",
    "web_page",
    "warehouse",
    #"customer",
    #"customer_address",
    #"customer_demographics",
    #"date_dim",
    #"household_demographics",
    #"item",
    "income_band",
    "promotion",
    #"reason",
    #"ship_mode",
    #"time_dim"
]

# Load TYPE_MAP from a type map file
def load_type_map(file_path):
    namespace = {}
    with open(file_path, "r", encoding="utf-8") as f:
        code = f.read()
        exec(code, {}, namespace)
    return namespace["TYPE_MAP"]

# Cast a row according to TYPE_MAP
def cast_row(row, type_map):
    casted = {}
    for col, val in zip(type_map.keys(), row):
        if val == "":
            continue
        caster = type_map[col]
        try:
            casted[col] = caster(val)
        except Exception:
            casted[col] = val
    return casted

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

for table in TABLES:
    data_file = os.path.join(DATA_DIR, table + ".dat")
    type_file = os.path.join(TYPEMAPS_DIR, f"{table}")

    if not os.path.exists(data_file):
        print(f"Data file not found for {table}, skipping...")
        continue
    if not os.path.exists(type_file):
        print(f"Type map file missing for {table}, skipping...")
        continue

    type_map = load_type_map(type_file)

    print(f"\nImporting {table} with fields: {list(type_map.keys())}")

    collection = db[table]
    batch = []

    # Read and insert data
    with open(data_file, newline='') as f:
        reader = csv.reader(f, delimiter='|')
        for row in reader:
            row_dict = cast_row(row, type_map)
            batch.append(row_dict)
            if len(batch) >= 1000:
                collection.insert_many(batch)
                batch = []
        if batch:
            collection.insert_many(batch)


print("\nAll tables imported successfully!")
