
select
@colnames
from (

select distinct t.*

from(

select distinct
SAFE_CAST(pdps.memberID as STRING) as ID,
pdps.*

from

(select distinct
pdp.memberID,
pdp.fullVisitorId,
date as date,
pdp.sessionId,
pdp.time_on_page,
pdp.productID,
pdp.isWishlist,
pdp.isShoppingBag,
pdp.isPDP_Men,
pdp.isPDP_Women,
SAFE_CAST(null AS INT64) as isEditorial,
pdp.isPLP_Men_brand,
pdp.isPLP_Women_brand
from `ds_sessions_value.app_page_raw` pdp

inner join (
select distinct sessionId
from `ds_sessions_value._app_session_md`
) s

on pdp.sessionId = s.sessionId
where (pdp.date >= '@start_date' and pdp.date <= '@end_date')) pdps

left outer join `ds_sessions_value._employees` em

on pdps.memberID = SAFE_CAST (em.memberID as STRING)
where em.memberID is null

) t

) x