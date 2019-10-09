
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
    sum(if (p.genderT='Men', p.priceCD, 0)) as pdp_price_cad_sum_CAT_men,
    sum(if (p.genderT='Women', p.priceCD, 0)) as pdp_price_cad_sum_CAT_women,
    avg(if (p.genderT='Men', p.priceCD, null)) as pdp_price_cad_avg_CAT_men,
    avg(if (p.genderT='Women', p.priceCD, null)) as pdp_price_cad_avg_CAT_women,
    sum(if (p.dept='RTW', p.priceCD, 0)) as pdp_price_cad_sum_CAT_rtw,
    sum(if (p.dept='ACC', p.priceCD, 0)) as pdp_price_cad_sum_CAT_acc,
    sum(if (p.dept='SHOES', p.priceCD, 0)) as pdp_price_cad_sum_CAT_shoes,
    sum(if (p.dept='BAGS', p.priceCD, 0)) as pdp_price_cad_sum_CAT_bags,
    avg(if (p.dept='RTW', p.priceCD, null)) as pdp_price_cad_avg_CAT_rtw,
    avg(if (p.dept='ACC', p.priceCD, null)) as pdp_price_cad_avg_CAT_acc,
    avg(if (p.dept='SHOES', p.priceCD, null)) as pdp_price_cad_avg_CAT_shoes,
    avg(if (p.dept='BAGS', p.priceCD, null)) as pdp_price_cad_avg_CAT_bags,
    COALESCE(sum(cast(p.genderT='Men' as INT64)), 0) as pdp_n_CAT_men,
    COALESCE(sum(cast(p.genderT='Women' as INT64)), 0) as pdp_n_CAT_women,
    COALESCE(sum(cast(p.dept='RTW' as INT64)), 0) as pdp_n_CAT_rtw,
    COALESCE(sum(cast(p.dept='ACC' as INT64)), 0) as pdp_n_CAT_acc,
    COALESCE(sum(cast(p.dept='SHOES' as INT64)), 0) as pdp_n_CAT_shoes,
    COALESCE(sum(cast(p.dept='BAGS' as INT64)), 0) as pdp_n_CAT_bags,
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
      sum(if (p.genderT='Men', p.priceCD, 0)) as pdp_price_cad_sum_lq_CAT_men,
      sum(if (p.genderT='Women', p.priceCD, 0)) as `pdp_price_cad_sum_lq_CAT_women`,
      avg(if (p.genderT='Men', p.priceCD, null)) as `pdp_price_cad_avg_lq_CAT_men`,
      avg(if (p.genderT='Women', p.priceCD, null)) as `pdp_price_cad_avg_lq_CAT_women`,
      sum(if (p.dept='RTW', p.priceCD, 0)) as `pdp_price_cad_sum_lq_CAT_rtw`,
      sum(if (p.dept='ACC', p.priceCD, 0)) as `pdp_price_cad_sum_lq_CAT_acc`,
      sum(if (p.dept='SHOES', p.priceCD, 0)) as `pdp_price_cad_sum_lq_CAT_shoes`,
      sum(if (p.dept='BAGS', p.priceCD, 0)) as `pdp_price_cad_sum_lq_CAT_bags`,
      avg(if (p.dept='RTW', p.priceCD, null)) as `pdp_price_cad_avg_lq_CAT_rtw`,
      avg(if (p.dept='ACC', p.priceCD, null)) as `pdp_price_cad_avg_lq_CAT_acc`,
      avg(if (p.dept='SHOES', p.priceCD, null)) as `pdp_price_cad_avg_lq_CAT_shoes`,
      avg(if (p.dept='BAGS', p.priceCD, null)) as `pdp_price_cad_avg_lq_CAT_bags`,
      STDDEV(p.priceCD) as pdp_price_cad_stdev_lq,
      COALESCE(sum(cast(p.genderT='Men' as INT64)), 0) as `pdp_n_lq_CAT_men`,
      COALESCE(sum(cast(p.genderT='Women' as INT64)), 0) as `pdp_n_lq_CAT_women`,
      COALESCE(sum(cast(p.dept='RTW' as INT64)), 0) as `pdp_n_lq_CAT_rtw`,
      COALESCE(sum(cast(p.dept='ACC' as INT64)), 0) as `pdp_n_lq_CAT_acc`,
      COALESCE(sum(cast(p.dept='SHOES' as INT64)), 0) as `pdp_n_lq_CAT_shoes`,
      COALESCE(sum(cast(p.dept='BAGS' as INT64)), 0) as `pdp_n_lq_CAT_bags`
from `ds_sessions_value._page_raw` pdp
left join  `ds_sessions_value._products` p
on pdp.productID = p.productID
where pdp.date >= '@last_quarter_date'
and pdp.productID is not null
group by pdp.ID) as lq

on t.ID = lq.ID

) x