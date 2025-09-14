CREATE TABLE IF NOT EXISTS tpcds.income_band (
    ib_income_band_sk int,
    ib_lower_bound int,
    ib_upper_bound int,
    PRIMARY KEY (ib_income_band_sk)
);
