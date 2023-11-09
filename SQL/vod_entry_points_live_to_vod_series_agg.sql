with base as (
select
    a.client_id,
    a.app_name,
    a.session_id,
    b.series_id
from
    "ODIN_PRD"."RPT"."BI_USER_ENGAGEMENT360" a
join
    "ODIN_PRD"."DW_ODIN"."CMS_EPISODE_DIM" b
on
    a.episode_id = b.episode_id
where
    a.screen_name in ('livecontentnow','livecontentlater')
and
    a.screen_element_name = 'watchfromstart'
and
    a.country = 'US'
and
    date_trunc('month',a.time_stamp) = '2023-04-01'
group by
    1,2,3,4
),

--- joins to all hourly agg to get vod tvms in the session for episodes with watch from start action
vod_tvms as (
select
    a.client_id,
    a.app_name,
    a.session_id,
    b.episode_id,
    sum(b.total_viewing_minutes) as tvm
from
    base a
join
    "ODIN_PRD"."RPT"."ALL_HOURLY_TVS_AGG" b
on
    a.session_id = b.session_id
join
    "ODIN_PRD"."DW_ODIN"."CMS_EPISODE_DIM" c
--- ensures series from episode dim are only ones present from livecontentlater and livecontentnow watchfromstart action selections above
on
    a.series_id = c.series_id
---matches the vod episodes for tvm aggregation to be in the series_id list from above
and
    b.episode_id = c.episode_id
where
    b.channel_id = 'vod'
and
    date_trunc('month',b.video_segment_begin_utc) = '2023-04-01'
and
    b.country = 'US'
group by
    1,2,3,4   
)

select
    app_name,
    count(distinct client_id) as num_users,
    count(distinct session_id) as num_sessions,
    sum(tvm) as tvm
from
    vod_tvms
group by
    1