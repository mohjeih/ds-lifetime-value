
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
and rand() < 0.5 ) bf  -- 0.1
limit 800000) -- 300000

) x