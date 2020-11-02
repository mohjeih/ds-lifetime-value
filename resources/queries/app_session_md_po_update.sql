
select
@colnames
from (

select distinct smd.*

from `ds_sessions_value._app_session_md_po` smd

inner join (

select distinct smdd.ID as ID
from `ds_sessions_value._app_session_md_po` smdd
where smdd.channel in ('SEM_PLA', 'SEM_Non_Branded', 'Display')
and (PARSE_DATE('%Y-%m-%d', smdd.date) >= DATE_ADD(CURRENT_DATE(), INTERVAL - 2 DAY)
         and PARSE_DATE('%Y-%m-%d', smdd.date) <= CURRENT_DATE())

) as tt

on smd.ID = tt.ID

) x