select  
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   mongodb.tpcds.web_sales ws1
  ,mongodb.tpcds.date_dim
  ,mongodb.tpcds.customer_address
  ,mongodb.tpcds.web_site
where CAST(d_date AS DATE) 
      between DATE '1999-04-01'
          and DATE '1999-04-01' + INTERVAL '60' DAY
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'WI'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and exists (select *
            from mongodb.tpcds.web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from cassandra.tpcds.web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
limit 100;

