
select
@colnames
from (

select distinct
      p.ID as productID,
      spt.name,
      pd.composition,
      p.productCode as productCode,
      p.dateCreation as prodCreationDate,
      concat(case when p.season = 1 then 'Spring Summer' else 'Fall Winter' end, ' ',
	      cast(case when p.currentYear >= 6 then p.currentYear + 2000 else p.currentYear + 2010 end as char)) as seasonYear,
      case when p.season = 1 then 'Spring Summer' else 'Fall Winter' end as season,
      pbr.ID as brandID,
      pbr.seo_keyword as brand,
      pbr.name as brand_name,
      case when p.gender = 'female' then 'Women' else 'Men' end as genderT,
      case when pc1.department = 1 then 'ACC'
      when pc1.department = 2 then 'BAGS'
      when pc1.department = 3 then 'RTW'
      else 'SHOES' end as dept,
	  coalesce(pc2.name,pc1.name) as category,
	  pc1.name AS subCategory,
      case when spt.name like 'SSENSE%%' then
           substring(spt.name,length(substring_index(spt.name,' ',2))+1, length(substring_index(spt.name,' ',3))
                      -length(substring_index(spt.name,' ',2)))
           else substring_index(spt.name,' ',1)
      end as color,
      spr.price as priceCD,
      case when p.display = 1 then '1' else '0' end as display,
      spt.description,
      case
            when p.stockForSale <= 0 then 0
            else p.stockForSale * p.display * p.image
      end as stockForSale
from products as p
join smart_product_translation AS spt on spt.smart_product_id = p.ID AND spt.language_id = 1
left join product_details as pd on pd.productID = p.ID
join smart_designer as pbr on p.brandID = pbr.ID
join category pc1 on pc1.ID = p.newCategoryID
left join category pc2 on pc2.ID = pc1.parent_ID
join (
    select product_id as productID, price
    from smart_price
    where currency_id = 1
    ) as spr
on p.ID = spr.productID
group By p.ID

) x