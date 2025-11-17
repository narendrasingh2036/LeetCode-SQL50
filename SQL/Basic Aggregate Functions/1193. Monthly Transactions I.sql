select 
DATE_FORMAT(trans_date, '%Y-%m') AS month,
country, 
count(*) as trans_count,
sum(
    CASE WHEN STATE='approved' THEN 1
    ELSE 0 END
) as approved_count,
sum(amount) as trans_total_amount,
sum(
    CASE WHEN STATE='approved' THEN AMOUNT
    ELSE 0 END
) as approved_total_amount
from Transactions
group by month , country 
order by month , country desc 