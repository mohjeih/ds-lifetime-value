
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
  memberID,
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

from `ds_sessions_value.app_session_raw`
where (SAFE_CAST(date as STRING) >= '@start_date' and SAFE_CAST(date as STRING) <= '@end_date')) s


left outer join `ds_sessions_value._employees` em
on s.memberID = SAFE_CAST (em.memberID as string)
where em.memberID is null
) as v

left join `ds_sessions_value._adwords` ad
on v.campaignId = ad.campaignId and v.adGroupId = ad.adGroupId and v.criteriaId = ad.criteriaId

) x