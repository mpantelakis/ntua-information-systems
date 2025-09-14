CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk int,
    inv_item_sk int,
    inv_warehouse_sk int,
    inv_quantity_on_hand int,
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);
