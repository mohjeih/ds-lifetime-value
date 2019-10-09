
select
@colnames
from (

select distinct
sf.ID as ID,
pf.* except (ID),
pdpf.* except (ID),
sf.* except (ID)
from `ds_sessions_value._session_features` sf
left join `ds_sessions_value._page_features` pf
using (ID)
left join `ds_sessions_value._pdp_features` pdpf
using (ID)

) x