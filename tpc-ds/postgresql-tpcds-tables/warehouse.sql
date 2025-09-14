CREATE TABLE IF NOT EXISTS warehouse (
    w_warehouse_sk int,
    w_warehouse_id text,
    w_warehouse_name text,
    w_warehouse_sq_ft int,
    w_street_number text,
    w_street_name text,
    w_street_type text,
    w_suite_number text,
    w_city text,
    w_county text,
    w_state text,
    w_zip text,
    w_country text,
    w_gmt_offset double precision,
    PRIMARY KEY (w_warehouse_sk)
);
