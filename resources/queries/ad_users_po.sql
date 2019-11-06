
select
@colnames
from (

select distinct
s.ID,
SAFE_CAST (s.campaignId as string) as campaignId,
SAFE_CAST (s.adGroupId as string) as adGroupId,
SAFE_CAST (s.criteriaId as string) as criteriaId
from `ds_sessions_value._session_md` s
join `ds_sessions_value._brx_features_po` bs
on s.memberID = bs.ID
where s.campaignId is not null

) x