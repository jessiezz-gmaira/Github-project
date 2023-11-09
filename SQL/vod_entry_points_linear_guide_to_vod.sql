--- selects all user sessions who visited a 'livecontentnow' or 'livecontentlater' screen and selected 'watchfromstart', gathering client session and episode id for the content selected, then joins to all_hourly to get tvms for that episode from that session
with linear_guide_to_vod as (
select
    a.client_id,
    c.app_name,
    a.session_id,
    a.episode_id,
    sum(b.total_viewing_minutes) as tvm    
from
    "ODIN_PRD"."RPT"."BI_USER_ENGAGEMENT360" a
join
    "ODIN_PRD"."STG"."DEVICE_MAPPING" c
on
    a.app_name = c.sub_app_name
join
    "ODIN_PRD"."RPT"."ALL_HOURLY_TVS_AGG" b
on
    a.session_id = b.session_id and a.episode_id = b.episode_id
where
    a.screen_name in ('livecontentnow','livecontentlater')
and
    a.screen_element_name = 'watchfromstart'
and
    a.country = 'US'
and
    date_trunc('month',a.time_stamp) = '2023-04-01'
and
    b.country = 'US'
and
    date_trunc('month',b.video_segment_begin_utc) = '2023-04-01'
and
    b.channel_id = 'vod'
group by
    1,2,3,4
)

select
    app_name,
    count(distinct client_id) as num_users,
    count(distinct session_id) as num_sessions,
    sum(tvm) as tvm
from
    linear_guide_to_vod
group by
    1