select
@colnames
from (
select distinct
     ID, date, isMD, wave, season, currentYear
from pr_markdown_info
where (date >= '@start_date' and date <= '@end_date')
) x