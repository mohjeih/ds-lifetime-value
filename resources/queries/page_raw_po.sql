
select
@colnames
from (

select distinct

case
when pdmc.memberID is not null and pdmc.conversion = 1 then pdmc.memberID
else pdmc.fullVisitorId
end as ID,
pdmc.* except(conversion)

from(

select distinct
case
when pdc.memberID is null and pdc.conversion = 1 and u.memberID is not null then SAFE_CAST(u.memberID as STRING)
else pdc.memberID
end as memberID,
pdc.* except(memberID)

from

(select distinct
pd.*,
case
when inv.invoiceID is not null then 1
else 0
end as conversion

from

(select distinct pdpsm.*

from

(select distinct
lnk.memberID,
pdps.*

from

(select distinct
pdp.fullVisitorId,
pdp.date,
pdp.sessionId,
pdp.time_on_page,
pdp.productID,
pdp.isWishlist,
pdp.isShoppingBag,
pdp.isPDP_Men,
pdp.isPDP_Women,
pdp.isEditorial,
pdp.isPLP_Men_brand,
pdp.isPLP_Women_brand
from `ds_user_data.page_raw` pdp

inner join (
select distinct sessionId
from `ds_sessions_value._session_md`
) s

on pdp.sessionId = s.sessionId
where (pdp.date >= '@start_date' and pdp.date <= '@end_date')) pdps

left join

(
select distinct
  fullVisitorId,
  ARRAY_AGG(memberID
  ORDER BY
    memberID DESC
  LIMIT
    1)
    [
OFFSET
  (0)] AS memberID
FROM (
  SELECT
    fullVisitorId,
    c.value AS memberID
  FROM
    `843777.ga_sessions_*`,
    UNNEST(customDimensions) AS c
  WHERE
    _TABLE_SUFFIX >= FORMAT_DATETIME("%Y%m%d",
      DATETIME_SUB(CURRENT_DATETIME(),
        INTERVAL 4 hour))
    AND c.index = 9
    AND SAFE_CAST(c.value AS FLOAT64) IS NOT NULL
  UNION ALL
  SELECT
    *
  FROM
    `843777.lnk_fullVisitorId_memberID_unique`)
GROUP BY
  fullVisitorId
) lnk

on pdps.fullVisitorId =  lnk.fullVisitorId) pdpsm

left outer join `ds_sessions_value._employees` em

on pdpsm.memberID = SAFE_CAST (em.memberID as STRING)
where em.memberID is null) as pd

left join

(select distinct fullVisitorId, string_agg(invoiceID) as invoiceID
from `ds_sessions_value._invoices_po`
group by fullVisitorId ) as inv

on pd.fullVisitorId = inv.fullVisitorId) as pdc

left join `ds_sessions_value._users` u

on pdc.fullVisitorId = u.fullVisitorId

) as pdmc

) x
