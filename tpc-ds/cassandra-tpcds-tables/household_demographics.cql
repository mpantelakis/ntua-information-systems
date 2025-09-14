CREATE TABLE IF NOT EXISTS tpcds.household_demographics (
    hd_demo_sk int,
    hd_income_band_sk int,
    hd_buy_potential text,
    hd_dep_count int,
    hd_vehicle_count int,
    PRIMARY KEY (hd_demo_sk)
);
