
select
@colnames
from (

select CampaignId as campaignId,
AdGroupId as adGroupId,
boomUserlistId,
UserListName as userListName
from `ds_sessions_value.audiences_table`
-- where UserListName = 'G3_B0_HTOS_FPV_QSsup8_Vinf9'
-- or UserListName = 'G1_B0_HTOS_HPV_MSQ_LV'
-- or UserListName like '%Polyvore%'

) x
