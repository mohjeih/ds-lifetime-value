
select
@colnames
from (

select
invc.fullVisitorId,
PARSE_DATE('%Y%m%d', invc.transactionDate) as date,
invc.transactionId as invoiceID,
invc.channel
from `ds_marketing.p_invoice_channel_device` invc

inner join(
select distinct fullVisitorId
from `ds_marketing.p_invoice_channel_device` inv
where inv.channel in ('SEM_PLA', 'SEM_Non_Branded', 'Display')
and (PARSE_DATE('%Y%m%d', inv.transactionDate) >= '@start_date'
and PARSE_DATE('%Y%m%d', inv.transactionDate) <= '@end_date')
) as t
on invc.fullVisitorId = t.fullVisitorId

where (PARSE_DATE('%Y%m%d', invc.transactionDate) >= '@start_date'
and PARSE_DATE('%Y%m%d', invc.transactionDate) <= '@end_date')

) x