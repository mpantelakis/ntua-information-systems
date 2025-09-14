CREATE TABLE IF NOT EXISTS tpcds.customer_demographics (
    cd_demo_sk int,
    cd_gender text,
    cd_marital_status text,
    cd_education_status text,
    cd_purchase_estimate int,
    cd_credit_rating text,
    cd_dep_count int,
    cd_dep_employed_count int,
    cd_dep_college_count int,
    PRIMARY KEY (cd_demo_sk)
);
