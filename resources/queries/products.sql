
select
@colnames
from (

select distinct
      p.ID as productID,
      coalesce(spt.name, p.name) as name,
      pd.composition,
      p.productCode as productCode,
      p.dateCreation as prodCreationDate,
      concat(case when p.season = 1 then 'Spring Summer' else 'Fall Winter' end, ' ',
	      cast(case when p.currentYear >= 6 then p.currentYear + 2000 else p.currentYear + 2010 end as char)) as seasonYear,
      case when p.season = 1 then 'Spring Summer' else 'Fall Winter' end as season,
      sd.ID as brandID,
      sd.seo_keyword as brand,
      sd.name as brand_name,

      CASE WHEN (spc1.department in (1,2,3,4)) and (p.gender = 'female') THEN 'Women'
             WHEN (spc1.department in (1,2,3,4)) and (p.gender = 'male') THEN 'Men'
             WHEN (spc1.department NOT in (1,2,3,4)) then 'Everything_Else'
             ELSE 'Men' END AS genderT,
 	spc1.ID AS subcategory_id,
 	spc1.name AS subCategory,

    CASE WHEN spc2.parent_id is not NULL THEN spc2.ID
			 ELSE spc1.ID END AS category_id,
	CASE WHEN spc2.parent_id is not NULL THEN spc2.name
			 ELSE spc1.name END AS category,

		CASE WHEN spc3.ID is not NULL THEN spc3.ID
			 WHEN (spc3.ID is NULL) and (spc2.ID is not NULL) THEN spc2.ID
			 ELSE spc1.ID END AS department_id,
		CASE WHEN spc3.ID is not NULL THEN spc3.name
			 WHEN (spc3.ID is NULL) and (spc2.ID is not NULL) THEN spc2.name
			 ELSE spc1.name END AS dept,

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
JOIN smart_product spd on spd.ID = p.ID
JOIN product_brands pb ON pb.ID = p.brandID
JOIN smart_designer sd ON sd.ID = spd.designer_id
JOIN smart_category spc1 ON spc1.ID = spd.category_id
LEFT OUTER JOIN smart_category spc2 ON spc2.ID = spc1.parent_id
LEFT OUTER JOIN smart_category spc3 ON spc3.ID = spc2.parent_id

left join smart_product_translation AS spt on spt.smart_product_id = p.ID AND spt.language_id = 1
left join product_details as pd on pd.productID = p.ID

join (
    select product_id as productID, price
    from smart_price
    where currency_id = 1
    ) as spr
on p.ID = spr.productID

group By p.ID

) x