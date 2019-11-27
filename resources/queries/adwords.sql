select
@colnames
from (

select g.* except(adWord),
case
when REGEXP_CONTAINS(g.adWord, r"branded terms - ") then REGEXP_REPLACE(g.adWord, r"branded terms - ", r"branded terms")
else g.adWord
end as adWord

from(

select t.* except (adWord),
case
when REGEXP_CONTAINS(t.adWord, r"ã§") then REGEXP_REPLACE(t.adWord, r"ã§", r"c")
when REGEXP_CONTAINS(t.adWord, r"ç") then REGEXP_REPLACE(t.adWord, r"ç", r"c")
when REGEXP_CONTAINS(t.adWord, r"&#231") then REGEXP_REPLACE(t.adWord, r"&#231", r"c")
when REGEXP_CONTAINS(t.adWord, r"é") then REGEXP_REPLACE(t.adWord, r"é", r"e")
when REGEXP_CONTAINS(t.adWord, r"è") then REGEXP_REPLACE(t.adWord, r"è", r"e")
when REGEXP_CONTAINS(t.adWord, r"&#232;") then REGEXP_REPLACE(t.adWord, r"&#232;", r"e")
when REGEXP_CONTAINS(t.adWord, r"ô") then REGEXP_REPLACE(t.adWord, r"ô", r"o")
when REGEXP_CONTAINS(t.adWord, r"&amp;") then REGEXP_REPLACE(t.adWord, r"&amp;", r"&")
when REGEXP_CONTAINS(t.adWord, r"&#039;") then REGEXP_REPLACE(t.adWord, r"&#039;", r"'")
when REGEXP_CONTAINS(t.adWord, r"×") then REGEXP_REPLACE(t.adWord, r"×", r"x")
when REGEXP_CONTAINS(t.adWord, r"å") then REGEXP_REPLACE(t.adWord, r"å", r"a")
when REGEXP_CONTAINS(t.adWord, r"ñ") then REGEXP_REPLACE(t.adWord, r"ñ", r"n")
when REGEXP_CONTAINS(t.adWord, r"\+") then REGEXP_REPLACE(t.adWord, r"\+", r"")
when REGEXP_CONTAINS(t.adWord, r"ssense") then REGEXP_REPLACE(t.adWord, r"ssense", r"")
when REGEXP_CONTAINS(t.adWord, r"\-") then REGEXP_REPLACE(t.adWord, r"\+", r"")
else t.adWord
end as adword

from
(
select CampaignId as campaignId,
AdGroupId as adGroupId,
CriterionId as criteriaId,
AdvertisingChannelType as adChannelType,
CampaignName as campaignName,
lower(AdGroupName) as adGroupName,
case
when AdvertisingChannelType in ('DISPLAY', 'SEARCH') then lower(AdGroupName)
when AdvertisingChannelType in ('SHOPPING') and REGEXP_CONTAINS(lower(Criteria), r"brand==") then REGEXP_EXTRACT(lower(Criteria), r"brand==([^&\r\n]+)")
when AdvertisingChannelType in ('SHOPPING') and not REGEXP_CONTAINS(lower(Criteria), r"brand==") and REGEXP_CONTAINS(lower(Criteria), r"custom") then null
else  lower(Criteria)
end as adWord
from `Adwords.adwords_account_table`
where  AdvertisingChannelType in ('SHOPPING', 'DISPLAY', 'SEARCH')
) as t

where (t.adWord is not null and t.adWord != '*')

) as g

) x
