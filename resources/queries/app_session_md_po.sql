
select
@colnames
from (

select distinct
SAFE_CAST(sm.memberID as STRING) as ID,
sm.*,

from(

select distinct
    s.*,
    md.isMD as session_in_md,
    md.wave as session_md_wave,
    md.season as session_md_season
from `ds_sessions_value._app_session_raw_agg` s

inner join(
select distinct sc.memberID
from `ds_sessions_value._app_session_raw_agg` sc
where sc.channel in ('SEM_PLA', 'SEM_Non_Branded', 'Display')
and sc.date in (
    select distinct so.date as so_date
    from `ds_sessions_value._app_session_raw_agg` so
    where (PARSE_DATE('%Y-%m-%d', so.date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
          and PARSE_DATE('%Y-%m-%d', so.date) <= CURRENT_DATE())
          )
) as t

on s.memberID = t.memberID

join `ds_sessions_value._markdown` md
on s.date = md.date

where (s.date >= '@start_date' and s.date <= '@end_date')

) sm

where sm.sessionId not in (
            select su.sessionId
            from `ds_sessions_value._sessionId_update` su
            where  PARSE_DATE('%Y-%m-%d', su.date) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      )

) x