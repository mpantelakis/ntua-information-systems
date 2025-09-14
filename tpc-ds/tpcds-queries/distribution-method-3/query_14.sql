select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when CAST(d_date AS DATE) < DATE '2000-05-19'
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when CAST(d_date AS DATE) >= DATE '2000-05-19'
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from cassandra.tpcds.inventory
       ,mongodb.tpcds.warehouse
       ,postgresql.public.item
       ,postgresql.public.date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and CAST(d_date AS DATE) between DATE '2000-05-19' - INTERVAL '30' DAY
                     and DATE '2000-05-19' + INTERVAL '30' DAY
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100;

