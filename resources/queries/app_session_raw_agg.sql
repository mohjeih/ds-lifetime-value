
select
@colnames
from (

select distinct

v.*,
ad.adChannelType,
ad.adWord

from(

select distinct s.*

from(

select distinct
lnk.memberID,
tm.*

from

(select distinct
  fullVisitorId,
  visitId,
  SAFE_CAST(date as STRING) as date,
  sessionId,
  SAFE_CAST(null AS INT64) AS visitNumber,
  SAFE_CAST(null AS INT64) AS session_hits,
  SAFE_CAST(null AS INT64) AS session_pageviews,
  SAFE_CAST(null AS INT64) AS session_time_on_site,
  deviceCategory,
  operatingSystem,
  browser,
  mobileDeviceBranding,
  deviceLanguage,
  deviceCountry,
  subContinent,
  country,
  city,
  channel,
  campaignId,
  adGroupId,
  SAFE_CAST(null AS INT64) AS boomUserlistId,
  criteriaId,
  SAFE_CAST(null AS STRING) AS UserListName

from `ds_user_data.app_session_raw`
where (SAFE_CAST(date as STRING) >= '@start_date' and SAFE_CAST(date as STRING) <= '@end_date')) tm

left join

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
        AND platform != 'WEB'
       AND SAFE_CAST(user_id AS FLOAT64) IS NOT NULL
  UNION ALL
  SELECT
    *
  FROM
    `ssense-mobile-app.analytics_176714860.lnk_addId_memberID_unique`)
GROUP BY
  fullVisitorId
) lnk

on tm.fullVisitorId =  lnk.fullVisitorId

) s


left outer join `ds_sessions_value._employees` em
on s.memberID = SAFE_CAST (em.memberID as string)
where em.memberID is null
) as v

left join `ds_sessions_value._adwords` ad
on v.campaignId = ad.campaignId and v.adGroupId = ad.adGroupId and v.criteriaId = ad.criteriaId

) x