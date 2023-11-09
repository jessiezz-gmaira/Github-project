with daily as (
    SELECT
        date
        , app_name
        , user_type
        , content_type
        , count(distinct client_id) as DAUs
        , count(distinct case when reactivation = 'reactivated' then client_id end) as reactivated_client_count
        , count(distinct case when reactivation = 'MoM return' then client_id end) as MoM_return_client_count
        , count(distinct case when reg_status = 'new reg' then client_id end) as new_reg_client_count
        , count(distinct case when reg_status = 'return reg' then client_id end) as return_reg_client_count
        , sum(total_tvms) as tvms
        , sum(total_tvms)/DAUs as MPU
        , sum(days_since_last_visit) as days_last
        , sum(loggedin_tvms) as loggedin_tvms
        , sum(DISCOVERY_TVMS) as DISCOVERY_TVMS
        , sum(CONTINUED_TVMS) as CONTINUED_TVMS
        , sum(BINGE_TVMS) as BINGE_TVMS
        , sum(content_cnt) as content_cnt

    FROM SANDBOX.ANALYSIS_PRODUCT.SC_CLIENT_DAILY_VAL_GT_081823
    -- WHERE date between '2023-01-01' and current_date()
    WHERE date between '2023-01-01' and '2023-01-02'
    group by all
    -- order by app_name, date,user_type,content_type
)
, 
TTFF_DL as (
    SELECT
        day_date,
        app_name,
        user_type,
        content_type,
        
        deeplink_tvm,
        deeplink_sessions_counts,
        total_session_counts,
        total_tvm,
        ttff_tvm,
        ttff_session_counts,
        
        all_sessions,
        active_sessions,
        bounce_rate
  
    FROM
        SANDBOX.ANALYSIS_PRODUCT.SC_DEEPLINK_TTFF
    -- WHERE date between '2023-01-01' and current_date()
    WHERE day_date between '2023-01-01' and '2023-01-02'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),

new_user_agg as (
select
    date,
    app_name,
    'new' as user_type,
    'TTL' as content_type,
    new_users_above_fwad_threshold,
    new_users_below_fwad_threshold,
    new_users_above_fwtvm_threshold,
    new_users_below_fwtvm_threshold,
    new_users_fvw_day_one,
    new_users_fvw_week_one,
    new_users_fvw_month_one,
    new_users_no_vod_month_one
from
    "SANDBOX"."ANALYSIS_PRODUCT"."SC_NEW_USER_AGG"
where
    date between '2023-01-01' and '2023-02-28'
group by
    1,2,3,4,5,6,7,8,9,10,11,12
)
SELECT
    a.date,
    a.app_name,
    a.content_type,
    a.user_type,
    -- NULL as success,
    a.content_cnt,
    a.discovery_tvms,
    a.continued_tvms,
    a.binge_tvms,
    a.tvms,
    a.DAUs,
    a.days_last, -- doesnt this output need to be a median ? or are we doing that in tableau? i dont think we even can do that in tableua

    a.reactivated_client_count,
    a.MoM_return_client_count,
    a.new_reg_client_count,
    a.return_reg_client_count,

    td.deeplink_tvm,
        td.deeplink_sessions_counts,
        td.total_session_counts,
        td.total_tvm,
        td.ttff_tvm,
        td.ttff_session_counts,
        
        td.all_sessions,
        td.active_sessions,
        td.bounce_rate,
        
    nu.new_users_above_fwad_threshold,
    nu.new_users_below_fwad_threshold,
    nu.new_users_above_fwtvm_threshold,
    nu.new_users_below_fwtvm_threshold,
    nu.new_users_fvw_day_one,
    nu.new_users_fvw_week_one,
    nu.new_users_fvw_month_one,
    nu.new_users_no_vod_month_one

FROM daily a
JOIN TTFF_DL td
on
    a.date = td.day_date 
AND
    a.app_name= td.app_name 
AND
    a.content_type = td.content_type 
AND
    a.user_type = td.user_type
JOIN
    new_user_agg nu
on
    a.date = nu.date
and
    a.app_name = nu.app_name
and
    a.content_type = nu.content_type
and
    a.user_type = nu.user_type
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32