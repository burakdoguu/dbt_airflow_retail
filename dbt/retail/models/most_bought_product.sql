with orders as (

    select * from {{ ref('stg_orders') }}

),

product_category_map as (

    select * from {{ ref('stg_product_category_map') }}

),

category_map_join as (

    select     
        orders.event,
        orders.messageid,
        orders.userid,
        orders.productid,
        orders.quantity,
        orders.orderid,
        product_category_map.categoryid
    from orders
    left join product_category_map on orders.productid = product_category_map.productid

),

clean_table as (

    SELECT 
        event,
        messageid,
        userid,
        productid,
        quantity,
        orderid,
        categoryid
    FROM category_map_join
    WHERE messageid IS NOT NULL
        AND userid IS NOT NULL
        AND orderid IS NOT NULL
        AND productid IS NOT NULL
        AND quantity IS NOT NULL
    GROUP BY event,messageid,userid,orderid,productid,quantity,categoryid

),


row_number_bought AS(
  SELECT 
  ROW_NUMBER() OVER (PARTITION BY categoryid ORDER BY SUM(quantity) DESC) AS RN,
  productid, SUM(quantity) as total_bought,categoryid
  FROM clean_table
  GROUP BY productid, categoryid
),

sort_order_category as (
    SELECT productid,total_bought,categoryid, CAST(regexp_replace(categoryid, '.*-', '') AS INT) 
    FROM row_number_bought
    WHERE RN <=10
    order by 4
),

final as (

    select productid,total_bought,categoryid FROM sort_order_category

)

select * from final
