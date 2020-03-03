

select
@colnames
from (

select distinct sc.sessionId, sc.date
from `ds_sessions_value._session_raw_agg` sc
where sc.channel in ('SEM_PLA', 'SEM_Non_Branded', 'Display')
and sc.date = (select max(so.date) from `ds_sessions_value._session_raw_agg` so)

union all(
select distinct sc.sessionId, sc.date
from `ds_sessions_value._app_session_raw_agg` sc
where sc.channel in ('SEM_PLA', 'SEM_Non_Branded', 'Display')
and sc.date = (select max(so.date) from `ds_sessions_value._app_session_raw_agg` so))

) x