select  sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   postgresql.public.catalog_sales 
   ,postgresql.public.item 
   ,postgresql.public.date_dim
where
i_manufact_id = 283
and i_item_sk = cs_item_sk
and CAST(d_date AS DATE) between DATE '1999-02-22'
                 and DATE '1999-02-22' + INTERVAL '90' DAY
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt
     > (
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            postgresql.public.catalog_sales 
           ,postgresql.public.date_dim
         where
              cs_item_sk = i_item_sk
          and CAST(d_date AS DATE) between DATE '1999-02-22'
                 and DATE '1999-02-22' + INTERVAL '90' DAY
          and d_date_sk = cs_sold_date_sk
      )
limit 100;

