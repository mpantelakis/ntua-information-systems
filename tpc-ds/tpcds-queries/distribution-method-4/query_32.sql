select i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue 
      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	postgresql.public.store_sales
    	,postgresql.public.item 
    	,postgresql.public.date_dim
where 
	ss_item_sk = i_item_sk 
  	and i_category in ('Shoes', 'Music', 'Men')
  	and ss_sold_date_sk = d_date_sk
	and CAST(d_date AS DATE) between DATE '2000-01-05'
                 and DATE '2000-01-05' + INTERVAL '30' DAY
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;

