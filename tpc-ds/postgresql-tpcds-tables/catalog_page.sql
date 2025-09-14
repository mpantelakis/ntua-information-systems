CREATE TABLE IF NOT EXISTS catalog_page (
    cp_catalog_page_sk int,
    cp_catalog_page_id text,
    cp_start_date_sk int,
    cp_end_date_sk int,
    cp_department text,
    cp_catalog_number int,
    cp_catalog_page_number int,
    cp_description text,
    cp_type text,
    PRIMARY KEY (cp_catalog_page_sk)
);
