CREATE TABLE IF NOT EXISTS tpcds.web_page (
    wp_web_page_sk int,
    wp_web_page_id text,
    wp_rec_start_date timestamp,
    wp_rec_end_date timestamp,
    wp_creation_date_sk int,
    wp_access_date_sk int,
    wp_autogen_flag text,
    wp_customer_sk int,
    wp_url text,
    wp_type text,
    wp_char_count int,
    wp_link_count int,
    wp_image_count int,
    wp_max_ad_count int,
    PRIMARY KEY (wp_web_page_sk)
);
