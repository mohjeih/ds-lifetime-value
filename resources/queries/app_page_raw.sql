
select
@colnames
from (

select distinct t.*

from(

select distinct
SAFE_CAST(pdps.memberID as STRING) as ID,
pdps.* except(memberID)

from

(

select distinct
lnk.memberID,
pdpsp.*

from

(select distinct
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
where (pdp.date >= '@start_date' and pdp.date <= '@end_date')) pdpsp

join

(
SELECT DISTINCT
  appId as fullVisitorId,
  ARRAY_AGG(memberID
  ORDER BY
    memberID DESC
  LIMIT
    1)[
OFFSET
  (0)] AS memberID
FROM (
   SELECT
    user_pseudo_id AS appId,
    user_id AS memberID
  FROM
    `ssense-mobile-app.analytics_176714860.events_*`
  WHERE
    _TABLE_SUFFIX >= FORMAT_DATETIME("%Y%m%d",
      DATETIME_SUB(CURRENT_DATETIME(),
        INTERVAL 4 hour))
       AND SAFE_CAST(user_id AS FLOAT64) IS NOT NULL
  UNION ALL
  SELECT
    *
  FROM
    `ssense-mobile-app.analytics_176714860.lnk_addId_memberID_unique`)
GROUP BY
  fullVisitorId
) lnk

on pdpsp.fullVisitorId =  lnk.fullVisitorId) pdps

left outer join `ds_sessions_value._employees` em

on pdps.memberID = SAFE_CAST (em.memberID as STRING)
where em.memberID is null

) t

) x