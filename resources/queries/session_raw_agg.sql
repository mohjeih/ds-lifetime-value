
select
@colnames
from (

select distinct

v.*,
a.UserListName,
ad.adChannelType,
ad.adWord

from(

select distinct
u.*

from(

select distinct
    s.memberID,
    s.fullVisitorId,
    s.visitId,
    SAFE_CAST (s.date as string) as date,
    s.sessionId,
    min(s.visitNumber) as visitNumber,
    sum(s.session_hits) as session_hits,
    sum(s.session_pageviews) as session_pageviews,
    sum(s.session_time_on_site) as session_time_on_site,
    s.deviceCategory,
    s.operatingSystem,
    s.browser,
    s.mobileDeviceBranding,
    s.deviceLanguage,
    s.deviceCountry,
    s.subContinent,
    s.country,
    s.city,
    s.channel,
    s.campaignId,
    s.adGroupId,
    s.boomUserlistId,
    s.criteriaId

from(

select distinct
lnk.memberID,
tm.*

from

(select t.*  except(memberID,`date`, channel, campaignId, adGroupId, boomUserlistId, criteriaId),
first_value(`date` IGNORE NULLS) over (partition by sessionId order by `date`, visitId asc) date,
first_value(channel IGNORE NULLS) over (partition by sessionId order by `date`, visitId asc) channel,
first_value(campaignId IGNORE NULLS) over (partition by sessionId order by `date`, visitId asc) campaignId,
first_value(adGroupId IGNORE NULLS) over (partition by sessionId order by `date`, visitId asc) adGroupId,
first_value(boomUserlistId IGNORE NULLS) over (partition by sessionId order by `date`, visitId asc) boomUserlistId,
first_value(criteriaId IGNORE NULLS) over (partition by sessionId order by `date`, visitId asc) criteriaId
from `ds_sessions_value.session_raw` t
where (`date` >= '@start_date' and `date` <= '@end_date')) as tm

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

on tm.fullVisitorId =  lnk.fullVisitorId

) s

group by
s.memberID,
s.fullVisitorId,
s.visitId,
date,
s.sessionId,
s.deviceCategory,
s.operatingSystem,
s.browser,
s.mobileDeviceBranding,
s.deviceLanguage,
s.deviceCountry,
-- s.continent,
s.subContinent,
s.country,
s.city,
s.channel,
s.campaignId,
s.adGroupId,
s.boomUserlistId,
s.criteriaId

) as u
left outer join `ds_sessions_value._employees` em
on u.memberID = SAFE_CAST (em.memberID as string)
where em.memberID is null

) as v
left join `ds_sessions_value._audiences` a
on v.campaignId = a.campaignId and v.adGroupId = a.adGroupId and v.boomUserlistId = a.boomUserlistId
left join `ds_sessions_value._adwords` ad
on v.campaignId = ad.campaignId and v.adGroupId = ad.adGroupId and v.criteriaId = ad.criteriaId

) x
