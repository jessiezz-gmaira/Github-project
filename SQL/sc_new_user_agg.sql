-- CREATE OR REPLACE TABLE SANDBOX.ANALYSIS_PRODUCT.SC_NEW_USER_AGG_NN_082223 AS
--- pulls all users daily with total tvms tenure days and vod tvms
with base as (
select
    date,
    client_id,
    app_name,
    tenure_days,
    sum(case when content_type = 'TTL' then total_tvms end) as total_tvms,
    sum(case when content_type = 'vod' then total_tvms end) as vod_tvms
from
    "SANDBOX"."ANALYSIS_PRODUCT"."SC_CLIENT_DAILY_VAL_GT_081823"
group by
    1,2,3,4
),

--- pulls out new users with tenure of 0 on each day from the CTE above
new_user_base as (
select
    date,
    client_id,
    app_name,
    tenure_days,
    total_tvms,
    vod_tvms
from
    base
where
    tenure_days = 0
group by
    1,2,3,4,5,6
),

--- pulls all new users within the last 30 days 
new_users_last_month as (
select
    a.date,
    a.app_name,
    b.client_id
from
    base a
left join
    new_user_base b
on
    datediff('day',a.date,b.date) between -30 and 0
and
    a.app_name = b.app_name
group by
    1,2,3
),

--- calculates new user metrics after the last month of new users has been defined for a given day
new_users_with_metrics as (
select
    a.date,
    a.app_name,
    a.client_id,
    sum(case when b.client_id is not null and b.tenure_days between 0 and 6 then b.total_tvms end) as first_week_tvms,
    count(distinct case when b.client_id is not null and b.tenure_days between 0 and 6 then b.date end) as first_week_active_days,
    min(case when b.client_id is not null and b.tenure_days between 0 and 29 and b.vod_tvms > 0 then b.tenure_days end) as tenure_days_at_first_vod_watch
from
    new_users_last_month a
left join
    base b
on
    a.client_id = b.client_id
and
    a.app_name = b.app_name
---where a.client_id = '4660d6631715d00737a12729a6b05856b5be49ae'
group by
    1,2,3
)

--- counts users by threshold, first vod watch user groups counted separately
select
    date,
    app_name,
    count(distinct case when first_week_active_days >= 2 then client_id end) as new_users_above_fwad_threshold,
    count(distinct case when first_week_active_days < 2 then client_id end) as new_users_below_fwad_threshold,
    count(distinct case when first_week_tvms >= 60 then client_id end) as new_users_above_fwtvm_threshold,
    count(distinct case when first_week_tvms < 60 then client_id end) as new_users_below_fwtvm_threshold,
    count(distinct case when tenure_days_at_first_vod_watch = 0 then client_id end) as new_users_fvw_day_one,
    count(distinct case when tenure_days_at_first_vod_watch between 1 and 6 then client_id end) as new_users_fvw_week_one,
    count(distinct case when tenure_days_at_first_vod_watch between 7 and 29 then client_id end) as new_users_fvw_month_one,
    count(distinct case when tenure_days_at_first_vod_watch > 29 or tenure_days_at_first_vod_watch is null  then client_id end) as new_users_no_vod_month_one
from
    new_users_with_metrics
group by
    1,2