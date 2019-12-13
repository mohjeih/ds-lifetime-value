

select
@colnames
from (

select session_md.*

from

(

select distinct

case
when smd.memberID is not null and smd.conversion = 1 then smd.memberID
else smd.fullVisitorId
end as ID,
smd.* except(conversion)

from(

select distinct
case
when smc.memberID is null and smc.conversion = 1 and u.memberID is not null then SAFE_CAST(u.memberID as STRING)
else smc.memberID
end as memberID,
smc.* except(memberID)

from

(select distinct
sm.*,
case
when inv.invoiceID is not null then 1
else 0
end as conversion

from

(
select distinct
    s.*,
    md.isMD as session_in_md,
    md.wave as session_md_wave,
    md.season as session_md_season
    -- md.currentYear as session_md_year
from `ds_sessions_value._session_raw_agg` s

inner join(

select distinct sc.fullVisitorId
from `ds_sessions_value._session_raw_agg` sc
where sc.channel in ('SEM_PLA', 'SEM_Non_Branded', 'Display')
and sc.date in (
    select distinct so.date as so_date
    from `ds_sessions_value._session_raw_agg` so
    where (PARSE_DATE('%Y-%m-%d', so.date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
          and PARSE_DATE('%Y-%m-%d', so.date) <= CURRENT_DATE())
          )
) as t

on s.fullVisitorId = t.fullVisitorId

join `ds_sessions_value._markdown` md
on s.date = md.date
where (s.date >= '@start_date' and s.date <= '@end_date')
) as sm

left join

(select distinct fullVisitorId, string_agg(invoiceID) as invoiceID
from `ds_sessions_value._invoices_po`
group by fullVisitorId ) as inv

on sm.fullVisitorId = inv.fullVisitorId
) as smc

left join `ds_sessions_value._users` u

on smc.fullVisitorId = u.fullVisitorId

) as smd

) as session_md

where session_md.sessionId not in (
            select su.sessionId
            from `ds_sessions_value._sessionId_update` su
            where  PARSE_DATE('%Y-%m-%d', su.date) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      )

) x