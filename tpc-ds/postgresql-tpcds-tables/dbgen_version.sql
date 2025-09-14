CREATE TABLE IF NOT EXISTS dbgen_version (
    dv_version text,
    dv_create_date date,
    dv_create_time date,
    dv_cmdline_args text,
    PRIMARY KEY (dv_version, dv_create_date, dv_create_time)
);
