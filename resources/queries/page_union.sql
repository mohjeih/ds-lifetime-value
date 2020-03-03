
select
@colnames
from (

select distinct t.*

from(

select *
from `ds_sessions_value._page_raw`

union all(

select *
from `ds_sessions_value._app_page_raw`)

) t

) x