
select
@colnames
from (

select distinct
ID,

-- sum>0, binary
case when sum(isWishlist) > 0 then 1 else 0 end as isWishlist_bin,
case when sum(isShoppingBag) > 0 then 1 else 0 end as isShoppingBag_bin,
case when sum(isEditorial) > 0 then 1 else 0 end as isEditorial_bin,
case when sum(isPDP_Men) > 0 then 1 else 0 end as isPDP_Men_bin,
case when sum(isPDP_Women) > 0 then 1 else 0 end as isPDP_Women_bin,
case when sum(isPDP_EverythingElse) > 0 then 1 else 0 end as isPDP_EverythingElse_bin,
case when sum(isPLP_Men_brand) > 0 then 1 else 0 end as isPLP_Men_brand_bin,
case when sum(isPLP_Women_brand) > 0 then 1 else 0 end as isPLP_Women_brand_bin,

-- sum,
sum(isWishlist) as isWishlist_sum,
sum(isShoppingBag) as isShoppingBag_sum,
sum(isEditorial) as isEditorial_sum,
sum(isPDP_Men) as isPDP_Men_sum,
sum(isPDP_Women) as isPDP_Women_sum,
sum(isPDP_EverythingElse) as isPDP_EverythingElse_sum,
sum(isPLP_Men_brand) as isPLP_Men_brand_sum,
sum(isPLP_Women_brand) as isPLP_Women_brand_sum,
sum(isPLP_EverythingElse_brand) as isPLP_EverythingElse_brand_sum,

-- avg,
avg(isWishlist) as isWishlist_avg,
avg(isShoppingBag) as isShoppingBag_avg,
avg(isEditorial) as isEditorial_avg,
avg(isPDP_Men) as isPDP_Men_avg,
avg(isPDP_Women) as isPDP_Women_avg,
avg(isPDP_EverythingElse) as isPDP_EverythingElse_avg,
avg(isPLP_Men_brand) as isPLP_Men_brand_avg,
avg(isPLP_Women_brand) as isPLP_Women_brand_avg,
avg(isPLP_EverythingElse_brand) as isPLP_EverythingElse_brand_avg,

-- standard deviation,
STDDEV(isWishlist) as isWishlist_stdev,
STDDEV(isShoppingBag) as isShoppingBag_stdev,
STDDEV(isEditorial) as isEditorial_stdev,
STDDEV(isPDP_Men) as isPDP_Men_stdev,
STDDEV(isPDP_Women) as isPDP_Women_stdev,
STDDEV(isPDP_EverythingElse) as isPDP_EverythingElse_stdev,
STDDEV(isPLP_Men_brand) as isPLP_Men_brand_stdev,
STDDEV(isPLP_Women_brand) as isPLP_Women_brand_stdev,
STDDEV(isPLP_EverythingElse_brand) as isPLP_EverythingElse_brand_stdev,

-- max,
max(isWishlist) as isWishlist_max,
max(isShoppingBag) as isShoppingBag_max,
max(isEditorial) as isEditorial_max,
max(isPDP_Men) as isPDP_Men_max,
max(isPDP_Women) as isPDP_Women_max,
max(isPDP_EverythingElse) as isPDP_EverythingElse_max,
max(isPLP_Men_brand) as isPLP_Men_brand_max,
max(isPLP_Women_brand) as isPLP_Women_brand_max,
max(isPLP_EverythingElse_brand) as isPLP_EverythingElse_brand_max,

-- time related -> session level
sum(sum_time_on_page_in_session) as sum_time_time_on_page,
avg(sum_time_on_page_in_session) as avg_time_time_on_page,
stddev(sum_time_on_page_in_session) as stdev_time_time_on_page,
max(sum_time_on_page_in_session) as max_time_time_on_page,
min(sum_time_on_page_in_session) as min_time_time_on_page,

avg(avg_time_on_page_in_session) as avg_avg_time_on_page_in_session,
avg(stdev_time_on_page_in_session) as avg_stdev_time_on_page_in_session,
avg(max_time_on_page_in_session) as avg_max_time_on_page_in_session,
avg(min_time_on_page_in_session) as avg_min_time_on_page_in_session,

-- time related -> page level
sum(n_pages_in_session) as sum_n_pages_in_session,
avg(n_pages_in_session) as avg_n_pages_in_session,
stddev(n_pages_in_session) as stdev_n_pages_in_session,
max(n_pages_in_session) as max_n_pages_in_session,
min(n_pages_in_session) as min_n_pages_in_session

from (

select
    ID,
    sessionId,
    sum(time_on_page) as sum_time_on_page_in_session,
    avg(time_on_page) AS avg_time_on_page_in_session,
    STDDEV(time_on_page) AS stdev_time_on_page_in_session,
    max(time_on_page) AS max_time_on_page_in_session,
    min(time_on_page) AS min_time_on_page_in_session,
    count(*) as n_pages_in_session,
    SUM(isWishlist) AS isWishlist,
    SUM(isShoppingBag) AS isShoppingBag,
    SUM(isEditorial) AS isEditorial,
    SUM(isPDP_Men) AS isPDP_Men,
    SUM(isPDP_Women) AS isPDP_Women,
    SUM(isPDP_EverythingElse) As isPDP_EverythingElse,
    SUM(isPLP_Men_brand) AS isPLP_Men_brand,
    SUM(isPLP_Women_brand) AS isPLP_Women_brand,
    SUM(isPLP_EverythingElse_brand) AS isPLP_EverythingElse_brand
from `ds_sessions_value._page_raw`
where (date >= '@start_date' and date <= '@end_date')
group by ID, sessionId

) page_session
group by ID

) x