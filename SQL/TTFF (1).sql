---------------------------------------------------------------------------------------------------------------------
-- This is the overall --
---------------------------------------------------------------------------------------------------------------------
WITH x1 AS (
    SELECT
        biu.client_id
        ,session_id
        ,biu.app_name
        ,min(case when event_name in ('appLaunch','sessionReset') then time_stamp end) as app_start_time
        ,min(case when event_name = 'episodeStart' then time_stamp end) as first_episode_time
        ,datediff(milliseconds,app_start_time,first_episode_time)/1000 as TTFF
    FROM
        ODIN_PRD.RPT.BI_USER_ENGAGEMENT360          biu
        LEFT JOIN ODIN_PRD.STG.DEVICE_MAPPING       dem ON dem.sub_app_name = biu.app_name
    WHERE
        date_trunc('month',time_stamp) ='2022-11-01'
        AND LOWER(biu.country) = 'us'
        AND lower(dem.platform) in ('ctv','mobile')
    GROUP BY 1,2,3
    ORDER BY 1,3
)
SELECT
    case when TTFF < 0 then 'bad'
          when app_start_time is null and first_episode_time is null then 'bad'
          when app_start_time is not null and first_episode_time is null then 'bounced (no episode start)'
          when app_start_time is null and first_episode_time is not null then 'bad'
          when TTFF > 0 then 'good'
          when TTFF = 0 and app_start_time is not null and first_episode_time is not null then 'good'
          else 'has both' end as session_events_status
    ,avg(TTFF) as avg_TTFF
    ,count(distinct client_id) as user_count
    ,count(distinct session_id) as session_count
FROM
    x1
GROUP BY 1
Order by 1,2
;

---------------------------------------------------------------------------------------------------------------------
-- This is grouped at the app_name level --
---------------------------------------------------------------------------------------------------------------------
WITH x1 AS (
    SELECT
        biu.client_id
        ,session_id
        ,biu.app_name
        ,min(case when event_name in ('appLaunch','sessionReset') then time_stamp end) as app_start_time
        ,min(case when event_name = 'episodeStart' then time_stamp end) as first_episode_time
        ,datediff(milliseconds,app_start_time,first_episode_time)/1000 as TTFF
    FROM
        ODIN_PRD.RPT.BI_USER_ENGAGEMENT360          biu
        LEFT JOIN ODIN_PRD.STG.DEVICE_MAPPING       dem ON dem.sub_app_name = biu.app_name
    WHERE
        date_trunc('month',time_stamp) ='2022-11-01'
        AND LOWER(biu.country) = 'us'
        AND lower(dem.platform) in ('ctv','mobile')
    GROUP BY 1,2,3
    ORDER BY 1,3
)
SELECT
    app_name,
    case when TTFF < 0 then 'bad'
          when app_start_time is null and first_episode_time is null then 'bad'
          when app_start_time is not null and first_episode_time is null then 'bounced (no episode start)'
          when app_start_time is null and first_episode_time is not null then 'bad'
          when TTFF > 0 then 'good'
          when TTFF = 0 and app_start_time is not null and first_episode_time is not null then 'good'
          else 'has both' end as session_events_status
    ,avg(TTFF) as avg_TTFF
    ,count(distinct client_id) as user_count
    ,count(distinct session_id) as session_count
FROM
    x1
GROUP BY 1,2
Order by 1,3
