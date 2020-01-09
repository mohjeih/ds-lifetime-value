

select
@colnames
from (

select distinct smd.ID as ID, max(smd.date) as date
from `ds_sessions_value._session_md` smd
where smd.channel in ('SEM_PLA', 'SEM_Non_Branded', 'Display')
group by ID

) x