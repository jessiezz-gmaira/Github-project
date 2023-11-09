CREATE OR REPLACE TABLE SANDBOX.ANALYSIS_PRODUCT.SC_CLIENT_DAILY_VAL_GT_081823 AS

WITH dates as (
    SELECT '2023-01-01'::date as start_dt,
        '2023-01-30'::date as end_dt
)

, reg_users as (
    SELECT
        CLIENT_ID,
        MIN(first_time_client_user_pair_utc) as first_time_client_paired
    FROM SANDBOX.ENGINEERING.CLIENT_USER_MAPPING_VW
    where date_trunc('day',first_time_client_user_pair_utc) <= (select end_dt from dates)
    GROUP BY 1
)

, logged_tvms as (
    SELECT
        CLIENT_ID,
        case when channel_id = 'vod' then 'vod' else 'linear' end as content_type,
        date_trunc('day',VIDEO_SEGMENT_BEGIN_UTC) AS loggedin_day,
        sum(total_viewing_seconds)/60.0 as loggedin_tvms
    FROM SANDBOX.ENGINEERING.USER_VIDEO_SEGMENT_FACT_VW vsf
        LEFT JOIN ODIN_PRD.STG.DEVICE_MAPPING dm on vsf.app_name = dm.sub_app_name
    WHERE LOGGEDIN_STATUS_FLAG = TRUE
        AND date_trunc('day',video_segment_begin_utc) between (select start_dt from dates) and (select end_dt from dates)
        AND GEO_ALIGNED_FLAG = True
        AND EP_SOURCES_ALIGNED_FLAG = True
        AND TIMELINE_ALIGNED_FLAG = True
        AND lower(dm.app_name) IN ('samsungtizen/orsay','androidtv','xboxone','roku','viziovia/smartcast','googletv','tvos','contour','hisense','virginmedia','catalyst','comcastx1','comcastxclass','lgwebos','firetv','playstation')
    GROUP BY 1,2,3
)

, logged_tvms_TTL as (
    SELECT
        CLIENT_ID,
        'TTL' as content_type,
        loggedin_day,
        sum(loggedin_tvms) as loggedin_tvms
    FROM logged_tvms
    GROUP BY 1,2,3
)

SELECT
    ch.client_id
    ,ch.app_name
    ,ch.user_type
    ,ch.content_type
    ,ch.date
    ,ch.tenure_days
    ,date_trunc('day',first_time_client_paired)::date as first_time_client_paired
    ,case when datediff(day,first_time_client_paired,ch.date) <= 30 then 'new reg'
          when datediff(day,first_time_client_paired,ch.date) > 30 then 'return reg'
          else 'non reg' end as reg_status

    ,loggedin_tvms

    ,min(case when ch.content_type = 'vod' then ch.content_start_Time end) as vod_start
    ,sum(ch.content_tvms) as TOTAL_tvms
    ,count(distinct case when ch.content_watched_3min = 1 then ch.episode_id end) as content_cnt

    ,datediff('day',lag(ch.date) over (partition by ch.client_id order by ch.date),ch.date::date) as days_since_last_visit
    ,case when ch.user_type = 'return' and days_since_last_visit <= 30 then 'MoM return'
          when ch.user_type = 'return' and days_since_last_visit > 30 then 'reactivated'
          else null end as reactivation

    ,sum(case when content_order = 1  then content_tvms end) as discovery_tvms
    ,sum(case when content_order > 1  then content_tvms end) as continued_tvms
    ,sum(case when binge = 1 then content_tvms end) as binge_tvms

FROM
    SANDBOX.ANALYSIS_PRODUCT.SC_USER_CONTENT_HISTORY ch
    LEFT JOIN reg_users rcm on rcm.client_id = ch.client_id
    LEFT JOIN logged_tvms rvs on rvs.client_id = rcm.client_id
                             and ch.date = rvs.loggedin_day
                             and rvs.content_type = ch.content_type
WHERE date_trunc('day',content_start_time) between  (select start_dt from dates) and (select end_dt from dates)
GROUP BY 1,2,3,4,5,6,7,8,9

UNION ALL

SELECT
    ch.client_id
    ,ch.app_name
    ,ch.user_type
    ,'TTL' as content_type
    ,ch.date
    ,ch.tenure_days
    ,date_trunc('day',first_time_client_paired)::date as first_time_client_paired
    ,case when datediff(day,first_time_client_paired,ch.date) <= 30 then 'new reg'
          when datediff(day,first_time_client_paired,ch.date) > 30 then 'return reg'
          else 'non reg' end as reg_status

    ,loggedin_tvms as loggedin_tvms

    ,min(case when ch.content_type = 'vod' then ch.content_start_Time end) as vod_start
    ,sum(ch.content_tvms) as TOTAL_tvms
    ,count(distinct case when ch.content_watched_3min = 1 then ch.episode_id end) as content_cnt

    ,datediff('day',lag(ch.date) over (partition by ch.client_id order by ch.date),ch.date::date) as days_since_last_visit
    ,case when ch.user_type = 'return' and days_since_last_visit <= 30 then 'MoM return'
          when ch.user_type = 'return' and days_since_last_visit > 30 then 'reactivated'
          else null end as reactivation

    ,sum(case when content_order = 1  then content_tvms end) as discovery_tvms
    ,sum(case when content_order > 1  then content_tvms end) as continued_tvms
    ,sum(case when binge = 1 then content_tvms end) as binge_tvms

FROM
    SANDBOX.ANALYSIS_PRODUCT.SC_USER_CONTENT_HISTORY ch
    LEFT JOIN reg_users rcm on rcm.client_id = ch.client_id
    LEFT JOIN logged_tvms_TTL rvs on rvs.client_id = rcm.client_id
                             and ch.date = rvs.loggedin_day
WHERE date_trunc('day',content_start_time) between  (select start_dt from dates) and (select end_dt from dates)
GROUP BY 1,2,3,4,5,6,7,8,9

-- ORDER BY 1,date,content_type
;
