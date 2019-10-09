
select
@colnames
from (

select *
from `ds_sessions_value._brx_features`
where conversion_po = 1

union distinct

(select distinct bf.*
from(
select *
from `ds_sessions_value._brx_features`
where conversion_po != 1
and rand() < 0.1 ) bf
limit 400000)

) x