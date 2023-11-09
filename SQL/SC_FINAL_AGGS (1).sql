-- create or replace table SANDBOX.ANALYSIS_PRODUCT.SC_FINAL_AGG as

with

daily as (
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
, TTFF_DL as (
    SELECT
        date,
        app_name,
        user_type,
        content_type,
        ttff_tvm,
        ttff_session_counts,
        deeplink_tvm,
        deeplink_sessions_counts,

        all_session_count,
        active_session_count
    FROM
        SANDBOX.ANALYSIS_PRODUCT.SC_DEEPLINK_TTFF
    -- WHERE date between '2023-01-01' and current_date()
    WHERE date between '2023-01-01' and '2023-01-02'
    group by all
)
SELECT
    a.date ,
    a.app_name,
    a.content_type,
    a.user_type,
    -- NULL as success,
    content_cnt,
    discovery_tvms,
    continued_tvms,
    binge_tvms,
    tvms,
    DAUs,
    days_last, -- doesnt this output need to be a median ? or are we doing that in tableau? i dont think we even can do that in tableua

    reactivated_client_count,
    MoM_return_client_count,
    new_reg_client_count,
    return_reg_client_count,

    ttff_tvm,
    ttff_session_counts,
    deeplink_tvm,
    deeplink_sessions_counts,
    null as all_session_count,
    null as active_session_count

FROM daily a
JOIN TTFF_DL td on a.date = td.date AND
    a.app_name= td.app_name AND
    a.content_type = td.content_type AND
    a.user_type = td.user_type AND
    td.content_type is not null
group by all

UNION ALL

SELECT
    a.date ,
    a.app_name,
    a.content_type,
    a.user_type,
    -- success,
    sum(content_cnt) as sum_uniq_content,
    sum(discovery_tvms) as discovery_tvms,
    sum(continued_tvms) as continued_tvms,
    sum(binge_tvms) as binge_tvms,
    sum(tvms) as tvms,
    DAUs,
    days_last,

    reactivated_client_count,
    MoM_return_client_count,
    new_reg_client_count,
    return_reg_client_count,

    sum(ttff_tvm) as ttff_tvm,
    sum(ttff_session_counts) as ttff_session_counts,
    sum(deeplink_tvm) as deeplink_tvm,
    sum(deeplink_sessions_counts) as deeplink_sessions_counts,

    all_session_count,
    active_session_count

FROM daily a
JOIN TTFF_DL br on a.date = br.date AND
    a.app_name= br.app_name AND
    a.user_type = br.user_type AND
    br.content_type is null AND
    a.content_type = 'TTL'
group by all
    order by 2,1,4,3
;
