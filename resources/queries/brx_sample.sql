
select
@colnames
from (

select *
from `ds_sessions_value._brx_features_pt`
where conversion_po = 1

union distinct

(select distinct bf.*
from(
select *
from `ds_sessions_value._brx_features_pt`
where conversion_po != 1
and rand() < 0.5 ) bf  -- 0.5
limit 1200000) -- 1500000

) x