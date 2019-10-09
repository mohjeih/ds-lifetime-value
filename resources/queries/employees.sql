
select
@colnames
from (

select
    ID as memberID
from members
where email like '%%ssense.com'

) x