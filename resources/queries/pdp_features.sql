
select
@colnames
from (

select distinct t.ID,
  t.* except (ID),
  lq.* except (ID)

from

(select distinct
    pdp.ID,
    count(distinct pdp.productID) as pdp_n_product,
    sum(p.priceCD) as pdp_price_cad_sum,
    avg(p.priceCD) as pdp_price_cad_avg,
    min(p.priceCD) as pdp_price_cad_min,
    max(p.priceCD) as pdp_price_cad_max,
    STDDEV(p.priceCD) as pdp_price_cad_stdev,
    avg(if (p.genderT='Men', p.priceCD, null)) as price_avg_men,
    avg(if (p.genderT='Women', p.priceCD, null)) as price_avg_women,
    avg(if (p.genderT='Everything_Else', p.priceCD, null)) as price_avg_everything_else,
    avg(if (p.dept='CLOTHING', p.priceCD, null)) as price_avg_rtw,
    avg(if (p.dept='ACCESSORIES', p.priceCD, null)) as price_avg_acc,
    avg(if (p.dept='SHOES', p.priceCD, null)) as price_avg_shoes,
    avg(if (p.dept='BAGS', p.priceCD, null)) as price_avg_bags,
    avg(if (p.dept='HOME', p.priceCD, null)) as price_avg_home,
    avg(if (p.dept='ACTIVITY', p.priceCD, null)) as price_avg_activity,
    avg(if (p.dept='SELF-CARE', p.priceCD, null)) as price_avg_self_care,
    avg(if (p.dept='TECHNOLOGY', p.priceCD, null)) as price_avg_technology,
    avg(if (p.dept='PETS', p.priceCD, null)) as price_avg_pets,
    COALESCE(sum(cast(p.dept='CLOTHING' as INT64)), 0) as pdp_n_rtw,
    COALESCE(sum(cast(p.dept='ACCESSORIES' as INT64)), 0) as pdp_n_acc,
    COALESCE(sum(cast(p.dept='SHOES' as INT64)), 0) as pdp_n_shoes,
    COALESCE(sum(cast(p.dept='BAGS' as INT64)), 0) as pdp_n_bags,
    COALESCE(sum(cast(p.dept='HOME' as INT64)), 0) as pdp_n_home,
    COALESCE(sum(cast(p.dept='ACTIVITY' as INT64)), 0) as pdp_n_activity,
    COALESCE(sum(cast(p.dept='SELF-CARE' as INT64)), 0) as pdp_n_self_care,
    COALESCE(sum(cast(p.dept='TECHNOLOGY' as INT64)), 0) as pdp_n_technology,
    COALESCE(sum(cast(p.dept='PETS' as INT64)), 0) as pdp_n_pets,
    STRING_AGG(p.brand) as pdp_brand,
    STRING_AGG(p.category) as pdp_category
from `ds_sessions_value._page_raw` pdp
left join  `ds_sessions_value._products` p
on pdp.productID = p.productID
where (pdp.date >= '@start_date' and pdp.date <= '@end_date')
and pdp.productID is not null
group by pdp.ID) as t

left join

(select distinct
      ID,
      count(distinct pdp.productID) as pdp_n_product_lq,
      sum(p.priceCD) as pdp_price_cad_sum_lq,
      avg(p.priceCD) as pdp_price_cad_avg_lq,
      min(p.priceCD) as pdp_price_cad_min_lq,
      max(p.priceCD) as pdp_price_cad_max_lq,
      STDDEV(p.priceCD) as pdp_price_cad_stdev_lq,
      avg(if (p.genderT='Men', p.priceCD, null)) as price_avg_men_lq,
      avg(if (p.genderT='Women', p.priceCD, null)) as price_avg_women_lq,
      avg(if (p.genderT='Everything_Else', p.priceCD, null)) as price_avg_everything_else_lq,
      avg(if (p.dept='CLOTHING', p.priceCD, null)) as price_avg_rtw_lq,
      avg(if (p.dept='ACCESSORIES', p.priceCD, null)) as price_avg_acc_lq,
      avg(if (p.dept='SHOES', p.priceCD, null)) as price_avg_shoes_lq,
      avg(if (p.dept='BAGS', p.priceCD, null)) as price_avg_bags_lq,
      avg(if (p.dept='HOME', p.priceCD, null)) as price_avg_home_lq,
      avg(if (p.dept='ACTIVITY', p.priceCD, null)) as price_avg_activity_lq,
      avg(if (p.dept='SELF-CARE', p.priceCD, null)) as price_avg_self_care_lq,
      avg(if (p.dept='TECHNOLOGY', p.priceCD, null)) as price_avg_technology_lq,
      avg(if (p.dept='PETS', p.priceCD, null)) as price_avg_pets_lq,
      COALESCE(sum(cast(p.dept='CLOTHING' as INT64)), 0) as pdp_n_rtw_lq,
      COALESCE(sum(cast(p.dept='ACCESSORIES' as INT64)), 0) as pdp_n_acc_lq,
      COALESCE(sum(cast(p.dept='SHOES' as INT64)), 0) as pdp_n_shoes_lq,
      COALESCE(sum(cast(p.dept='BAGS' as INT64)), 0) as pdp_n_bags_lq,
      COALESCE(sum(cast(p.dept='HOME' as INT64)), 0) as pdp_n_home_lq,
      COALESCE(sum(cast(p.dept='ACTIVITY' as INT64)), 0) as pdp_n_activity_lq,
      COALESCE(sum(cast(p.dept='SELF-CARE' as INT64)), 0) as pdp_n_self_care_lq,
      COALESCE(sum(cast(p.dept='TECHNOLOGY' as INT64)), 0) as pdp_n_technology_lq,
      COALESCE(sum(cast(p.dept='PETS' as INT64)), 0) as pdp_n_pets_lq
from `ds_sessions_value._page_raw` pdp
left join  `ds_sessions_value._products` p
on pdp.productID = p.productID
where pdp.date >= '@last_quarter_date'
and pdp.productID is not null
group by pdp.ID) as lq

on t.ID = lq.ID

) x