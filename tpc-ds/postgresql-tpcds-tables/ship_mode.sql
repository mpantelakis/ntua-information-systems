CREATE TABLE IF NOT EXISTS ship_mode (
    sm_ship_mode_sk int,
    sm_ship_mode_id text,
    sm_type text,
    sm_code text,
    sm_carrier text,
    sm_contract text,
    PRIMARY KEY (sm_ship_mode_sk)
);
