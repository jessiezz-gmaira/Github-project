CREATE OR REPLACE table SANDBOX.ANALYSIS_PRODUCT.SCORECARD_NEW_USER_BASE_NN_ALL_DEVICES_072423 as
with base as (
select
    date_trunc('day',content_start_time) as watch_date,
    client_id,
    app_name,
    tenure_days,
    client_start_date,
    min(content_start_time) as day_start,
    min(case when content_type = 'vod' then content_start_time end) as vod_start,
    count(distinct episode_id) as num_episodes,
    sum(content_tvms) as tvms,
    sum(case when content_type = 'vod' then content_tvms end) as vod_tvms
from
    "SANDBOX"."ANALYSIS_PRODUCT"."SC_USER_CONTENT_HISTORY"
where
    date_trunc('day',content_start_time) between '2023-04-01' and '2023-07-31'
group by
    1,2,3,4,5
),

future_base as (
select
    watch_date,
    client_id,
    app_name,
    tenure_days,
    num_episodes,
    tvms,
    vod_tvms,
    day_start,
    vod_start
from
    base
group by
    1,2,3,4,5,6,7,8,9
)---,


---comb as (
select
    a.watch_date,
    a.client_id,
    a.app_name,
    a.client_start_date,
    a.num_episodes,
    a.tvms,
    a.vod_tvms,
    
    max(case when b.client_id is not null and b.tenure_days between 7 and 36 then 1 else 0 end) as return_flag_7d_36d,
    
    sum(case when b.client_id is not null and b.tenure_days between 0 and 6 then b.tvms end) as tvms_0d_6d,
    sum(case when b.client_id is not null and b.tenure_days between 7 and 36 then b.tvms end) as tvms_7d_36d,
    
    count(distinct case when b.client_id is not null and b.tenure_days between 0 and 6 then b.watch_date end) as num_active_days_0d_6d,    
    
    min(case when b.client_id is not null and b.tenure_days >= a.tenure_days then b.vod_start end) as first_vod_watch_ts,
    datediff('hour',a.client_start_date,first_vod_watch_ts) as hours_to_first_vod_watch,
    case when hours_to_first_vod_watch < 24 then 'first day'
    when hours_to_first_vod_watch >= 24 and hours_to_first_vod_watch < 168 then 'first week'
    when hours_to_first_vod_watch >= 168 and hours_to_first_vod_watch < 720 then 'first month'
    else 'no VOD in first month'
    end as first_vod_watch_buckets
from
    base a
left join
    future_base b
on
    a.client_id = b.client_id
and
    a.app_name = b.app_name
where
    a.tenure_days = 0
and
    date_trunc('month',a.watch_date) = '2023-04-01'
group by
    1,2,3,4,5,6,7
;