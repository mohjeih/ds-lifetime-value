
select
@colnames
from (

select distinct s.*

from(

select *
from `ds_sessions_value._session_md`

union all(

select *
from `ds_sessions_value._app_session_md`)

) s

) x