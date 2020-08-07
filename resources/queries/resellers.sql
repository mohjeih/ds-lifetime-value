select
@colnames
from (

select distinct
      memberID,
      max(confirmedFlag) as flagReseller
from  do_hvc_cart_items
where orderTime > DATE_SUB(CURDATE(), INTERVAL 104 WEEK)
group by memberID
having flagReseller = 1

) x