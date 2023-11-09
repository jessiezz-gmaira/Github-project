CREATE OR REPLACE table SANDBOX.ANALYSIS_PRODUCT.VODENTRIES_NN_NEW_062223 as
with base as (
SELECT 
    a.client_id, 
    a.session_id, 
    episode_id, 
    episode_name,
    allotment, 
    series_id, 
    series_name, 
    season_type, 
    c.app_name,
    live_flag, 
    ep_start, 
    tvs, 
    channels, 
    first_channel, 
    vod_tvm, 
    tvms
FROM 
    SANDBOX.ANALYSIS_PRODUCT.VODTVMS_NN_062123 a
JOIN 
    SANDBOX.ANALYSIS_PRODUCT.USERCHANNELS_NN_062223 b
on 
    a.client_id = b.client_id 
and 
    a.session_id = b.session_id
JOIN 
    "ODIN_PRD"."STG"."DEVICE_MAPPING" c 
on
    c.sub_app_name = a.app_name
group by
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),

 linear_base as (
select    
    a.client_id,    
    a.session_id,    
    a.event_name,
    a.screen_name,
    a.screen_element_name,
    a.event_occurred_utc,
    a.label,
    lead(a.label) ignore nulls over (partition by a.session_id order by a.event_occurred_utc) as label_lead
   --- lead(a.event_name) ignore nulls over (partition by a.session_id order by a.time_stamp) event_lead
from    
    SANDBOX.ENGINEERING.UX_EVENT_FACT_US_VW3 a
where
    ((a.screen_name in ('livecontentnow','livecontentlater','livechanneldetails')
and  
    a.screen_element_name in('watchfromstart','watchnow'))
  or a.event_name = 'vodEpisodeWatch')

and 
    date_trunc('month',a.event_occurred_utc) = '2023-05-01'

group by 1,2,3,4,5,6,7
),

label_agg as (
select
  a.client_id,
  a.session_id,
  a.event_name,
  a.label_lead as episode_id,
  sum(b.total_viewing_minutes) as tvm,
  min(b.video_segment_begin_utc) as ep_start
from linear_base a
  join    
    "ODIN_PRD"."RPT"."ALL_HOURLY_TVS_AGG" b
on    
    a.session_id = b.session_id
  and a.label_lead = b.episode_id
  where
    b.country = 'US'
and 
    date_trunc('month',b.video_segment_begin_utc) = '2023-05-01'
  group by 1,2,3,4
),

search as (
SELECT 
    client_id, 
    session_id, 
    search_dt, 
    episode_id, 
    series_name,
    CONVERT_TIMEZONE('America/New_York', 'UTC', TIMESTAMP_NTZ_FROM_PARTS(est_date, start_ts_est)) as start_ts
from 
    sandbox.analysis_product.vodsearch_nn_062223
where 
    rn5 = 1
group by
    1,2,3,4,5,6
), 

section_base as (
select
    client_id,
    session_id,
    match_number,
    sum(case when cl = 'BROWSE' or (cl = 'SECTION' and screen_element_name in ('vodl2','vodl2nav','herocarouseldetails')) then 1 else 0 end) as browse_cnt,
    max(case when cl = 'WATCH' then 1 else 0 end) as watch_flag
from SANDBOX.ANALYSIS_PRODUCT.VODSECTION_MATCHES_BASE_062223
  group by 1,2,3
),

section as (
select
    a.client_id,
    a.session_id,
    a.match_number,
    a.watch_ts,
    a.episode_id
from SANDBOX.ANALYSIS_PRODUCT.VODSECTION_MATCHES_BASE_062223 a
  join section_base b on a.client_id = b.client_id and a.session_id = b.session_id and a.match_number = b.match_number
where a.cl = 'WATCH'
  and b.browse_cnt > 0
  ---and b.search_flag = 0
group by 1,2,3,4,5
),

indicator_base as (
SELECT 
    a.client_id, 
    a.session_id, 
    a.episode_id, 
    episode_name,
    allotment, 
    series_id, 
    a.series_name,
    season_type,
    a.app_name,
    a.ep_start, 
    tvs, 
    channels, 
    vod_tvm, 
    tvms, 
    first_channel, 
    case when first_channel = 'vod' then 1
    when s2.episode_id = a.episode_id and s2.session_id = a.session_id and date_trunc('second',s2.watch_ts) = date_trunc('second',a.ep_start) then 2
    when s.episode_id = a.episode_id and s.session_id = a.session_id and date_trunc('second',s.start_ts) = date_trunc('second',a.ep_start)  then 3
    when l.episode_id = a.episode_id and l.session_id = a.session_id and date_trunc('second',l.ep_start) = date_trunc('second',a.ep_start) then 4
     end as entrypoint_flag
  
    
    
from 
    base a
LEFT JOIN 
    label_agg l 
on 
    a.client_id = l.client_id 
and 
    a.session_id = l.session_id 
and      
    a.episode_id = l.episode_id 
and 
    date_trunc('second',a.ep_start) = date_trunc('second',l.ep_start)
LEFT JOIN 
    search s 
on 
    a.client_id = s.client_id 
and 
    a.session_id = s.session_id 
and
    a.episode_id = s.episode_id 
and 
    date_trunc('second',a.ep_start) = date_trunc('second',s.start_ts)
LEFT JOIN 
    section s2 
on 
    a.client_id = s2.client_id 
and 
    a.session_id = s2.session_id
and
    a.episode_id = s2.episode_id
and
    date_trunc('second',a.ep_start) = date_trunc('second',s2.watch_ts)
where
    a.live_flag = 0
and
    lower(a.app_name) not in ('ios','tvos','airplay')
group by
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)---,

---binge_base as (
select
    client_id,
    session_id,
    episode_id,
    
    --- gets previous episode started
    lag(episode_id) ignore nulls over (partition by client_id,session_id order by ep_start)  as lag_episode_id,
    allotment,
    series_id,
    
    --- gets series id of previous episode started
    lag(series_id) ignore nulls over (partition by client_id,session_id order by ep_start)  as lag_series_id,
    season_type,
    app_name,
    ep_start,
    tvs,
    entrypoint_flag,
    
    --- when the series_id of the current rows episode start matches the previous series id, gathers the previous entrypoint flag in the session ordered by episode start
    case when series_id = lag_series_id then lag(entrypoint_flag) ignore nulls over (partition by client_id,session_id order by ep_start) end as lag_entrypoint_flag,
    
    --- combines the original entrypoint flag field with the lagged one. prioritizes original
    coalesce(entrypoint_flag,lag_entrypoint_flag) as comb_entrypoint_flag
from
    --"SANDBOX"."ANALYSIS_PRODUCT"."VODENTRIES_NN_061223" -- original table without pv_vodhome for vodsection funnel start
    indicator_base
--where session_id = 'd289b272-d64f-11ed-a62f-9af2467e8675'
group by
    1,2,3,5,6,8,9,10,11,12