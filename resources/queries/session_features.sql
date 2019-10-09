
select
@colnames
from (

select distinct sf.* except(conversion),
case
when regexp_contains(sf.conversion, "1") then 1
else 0
end as conversion

from

(select distinct
    s.ID,
    s.* except(ID),
    lq.* except(ID)

from

(select distinct
    ID,
    max(distinct visitId) as recent_session_epoch,
    min(distinct visitId) as session_epoch_min,
    avg(distinct visitId) as session_epoch_avg,
    stddev(distinct visitId) as session_epoch_stdev,
    24*3600*(DATE_DIFF(PARSE_DATE('%Y-%m-%d', '@end_date'), PARSE_DATE('%Y-%m-%d', min(distinct date)), DAY)) as sec_since_first_session,
    24*3600*(DATE_DIFF(PARSE_DATE('%Y-%m-%d', '@end_date'), PARSE_DATE('%Y-%m-%d', max(distinct date)), DAY)) as sec_since_last_session,
    count(distinct sessionId) as total_session_in_period,
    max(visitNumber) as total_session_in_life,
    sum(session_hits) as session_hits_sum,
    avg(session_hits) as session_hits_avg,
    sum(session_pageviews) as session_pageviews_sum,
    avg(session_pageviews) as session_pageviews_avg,
    sum(session_time_on_site) as session_time_on_site_sum,
    avg(session_time_on_site) as session_time_on_site_avg,
    STRING_AGG(deviceCategory) as deviceCategory,
    STRING_AGG(operatingSystem) as operatingSystem,
    STRING_AGG(browser) as browser,
    STRING_AGG(mobileDeviceBranding) as mobileDeviceBranding,
    STRING_AGG(deviceLanguage) as deviceLanguage,
    STRING_AGG(deviceCountry) as deviceCountry,
--     STRING_AGG(continent) as continent,
    STRING_AGG(subContinent) as subContinent,
    STRING_AGG(country) as country,
    STRING_AGG(city) as city,
    STRING_AGG(channel) AS channel,
    STRING_AGG(UserListName) as UserListName,
    STRING_AGG(adChannelType) as adChannelType,
    STRING_AGG(adWord) as adWord,
    sum(session_in_md) as session_in_md,
    avg(session_in_md) as session_in_md_avg,
    STRING_AGG(SAFE_CAST(session_md_season as string)) as session_md_season,
    STRING_AGG(SAFE_CAST(session_md_wave as string)) as session_md_wave,
    STRING_AGG(SAFE_CAST(session_md_year as string)) as session_md_year,
    STRING_AGG(SAFE_CAST(conversion as string)) as conversion
from `ds_sessions_value._session_md`
where (date >= '@start_date' and date <= '@end_date')
group by ID) as s

left join

(select distinct
    ID,
    avg(distinct visitId) as session_epoch_avg_lq,
    min(distinct visitId) as session_epoch_min_lq,
    count(distinct sessionId) as total_session_in_period_lq,
    sum(session_hits) as session_hits_sum_lq,
    avg(session_hits) as session_hits_avg_lq,
    sum(session_pageviews) as session_pageviews_sum_lq,
    avg(session_pageviews) as session_pageviews_avg_lq,
    sum(session_time_on_site) as session_time_on_site_sum_lq,
    avg(session_time_on_site) as session_time_on_site_avg_lq
from  `ds_sessions_value._session_md`
where date >= '@last_quarter_date'
group by ID) as lq

on s.ID = lq.ID) as sf

) x