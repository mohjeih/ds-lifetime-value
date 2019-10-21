
select
@colnames
from (

select CampaignId as campaignId,
AdGroupId as adGroupId,
boomUserlistId,
UserListName as userListName
from `ds_sessions_value.audiences_table`

) x
