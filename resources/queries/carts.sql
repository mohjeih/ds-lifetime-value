
select
@colnames
from (

select distinct
	   c.memberID,
	   c.ID as cartID,
	   date(c.invoiceDate) as cartDate,
	   c.invoice as invoiceID,
	   CASE WHEN spc3.ID is not NULL THEN spc3.name
	   WHEN (spc3.ID is NULL) and (spc2.ID is not NULL) THEN spc2.name
	   ELSE spc1.name END AS dept,
	   CASE WHEN spc2.parent_id is not NULL THEN spc2.name
	   ELSE spc1.name END AS category,
	   spc1.name AS subCategory,
	   sd.seo_keyword as brand,
	   ci.productID as productID,
	   ci.sizeId as sizeId,
	   CASE WHEN (spc1.department in (1,2,3,4)) and (p.gender = 'female') THEN 'Women'
       WHEN (spc1.department in (1,2,3,4)) and (p.gender = 'male') THEN 'Men'
       WHEN (spc1.department NOT in (1,2,3,4)) then 'Everything_Else'
       ELSE 'Men' END AS genderT,
	   concat(case when p.season = 1 then 'SS' else 'FW' end,
	   cast(case when p.currentYear >= 6 then p.currentYear + 2000 else p.currentYear + 2010 end as char)) as seasonYear,
       case when (ci.salePercentage >= 0 and ci.salePercentage < 1) then round(ci.salePercentage, 2)
       when ci.salePercentage >= 1 then round(ci.salePercentage/100, 2)
       else 0 end as salePercentage,
	   case when ci.salePercentage <= 0 then 0
	   when ci.salePercentage > 0 and ci.salePercentage < 0.15 then 0
	   when ci.salePercentage >= 0.15 and ci.salePercentage <= 0.99 then 1
	   when ci.salePercentage >= 10 then 1
	   else 0 end as markdownFlag,
	   case when cf.cartID is not null then 1 else 0 end as fraudFlag,
	   round(c.total/er.rate, 2) as totalCAD,
	   sum(ci.quantity) as nP,
	   sum(case when ri.originalOrderItemID is not null then ci.quantity else 0 end) as nR,
	   round(sum(ci.quantity*(ci.price-coalesce(ci.discount,0))/er.rate),2) as valueCAD_P,
	   round(sum(case when ri.originalOrderItemID is not null then ci.quantity*(ci.price-coalesce(ci.discount,0))/er.rate else 0 end),2) as valueCAD_R,
	   sum(ci.quantity*costs.cost) as cogsCAD_P,
	   sum(case when ri.originalOrderItemID is not null then ci.quantity*costs.cost else 0 end) as cogsCAD_R,
	   lr.reason as return_reason
from carts c
join cart_items ci on ci.cartID = c.ID
join products p on p.ID = ci.productID
join smart_product spd on spd.ID = p.ID
join product_brands pb on pb.ID = p.brandID
join smart_designer sd on sd.ID = spd.designer_id
join smart_category spc1 on spc1.ID = spd.category_id
left outer join smart_category spc2 on spc2.ID = spc1.parent_id
left outer join smart_category spc3 on spc3.ID = spc2.parent_id
join exchange_rates er on er.targetCurrencyID = c.currencyID and er.date = date(c.invoiceDate)
join order_status cs on cs.cart_id = c.ID and cs.active = 1 and cs.status <> 'cancelled'
join costs costs on costs.productSizeID = ci.sizeID
left join return_items ri on ri.originalOrderItemID = ci.ID
left join cart_return_items cri on ri.returnRequestItemID = cri.ID
left join list_reasons lr on cri.reasonID = lr.ID
left join cart_frauds cf on c.ID = cf.cartID and reasonCode in ('19','20','42','71','73','79')
join members m on m.ID = c.memberID
where c.origin in ('userOrder', 'phoneOrder')
	   and (c.invoiceDate >= '@start_date' and c.invoiceDate <= '@end_date')
	   and ci.quantity > 0
	   and c.memberEmail not like '%ssense.com'
group by c.ID,ci.productID,ci.sizeId,c.invoiceDate,genderT,category,dept,brand,seasonYear
having (valueCAD_P >= 1)
	   and (nP - nR >= 0)
	   and (fraudFlag != 1)
order by memberID, cartDate

) x