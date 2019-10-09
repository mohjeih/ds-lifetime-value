
select
@colnames
from (

select distinct
brx.*,
case
when inv.invoiceID is not null then 1
else 0
end as conversion_po

from

(select distinct
sf.ID as ID,
pf.* except (ID),
pdpf.* except (ID),
sf.* except (ID)
from `ds_sessions_value._session_features` sf
left join `ds_sessions_value._page_features` pf
using (ID)
left join `ds_sessions_value._pdp_features` pdpf
using (ID)
) as brx

left join

(

select distinct
case
when inv_pom.memberID is not null then inv_pom.memberID
else inv_pom.fullVisitorId
end as ID,
inv_pom.invoiceID

from

(select distinct
lnk.memberID,
inv_po.fullVisitorId,
inv_po.invoiceID

from

(select distinct
fullVisitorId, string_agg(invoiceID) as invoiceID
from `ds_sessions_value._invoices_po`
group by fullVisitorId) as inv_po

left join `843777.lnk_fullVisitorId_memberID_unique` lnk

on inv_po.fullVisitorId =  lnk.fullVisitorId) as inv_pom

) as inv

on brx.ID = inv.ID

) x