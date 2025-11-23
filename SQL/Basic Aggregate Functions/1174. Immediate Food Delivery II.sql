SELECT 
ROUND((SUM(ORDER_TYPE)/COUNT(ORDER_TYPE))*100,2) AS immediate_percentage 
FROM (SELECT *,
    CASE WHEN order_date=customer_pref_delivery_date THEN 1
    ELSE 0 END ORDER_TYPE 
FROM (SELECT *,
    ROW_NUMBER() OVER(PARTITION BY customer_id order by order_date asc) as ROW_NUM
FROM Delivery ) A WHERE ROW_NUM=1
) B