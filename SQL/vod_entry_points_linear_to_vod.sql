CREATE OR REPLACE table SANDBOX.ANALYSIS_PRODUCT.LINEAR_TO_VOD_062223 as

--- gathers the watchfromstart and watchnow events from the linear pages and the content being browsed from the json extension
with linear_base as (
select    
    a.client_id,    
    a.session_id,    
    a.event_name,
    a.screen_name,
    a.screen_element_name,
    a.event_occurred_utc,
    coalesce(
        lower(json_extract_path_text(a.json_extensions, 'data.screenElementDetail[0].user_category_id')),
        lower(json_extract_path_text(a.json_extensions, 'data.screenElementDetail[0].category_id')),
        lower(json_extract_path_text(a.json_extensions, 'data.screenElementDetail.user_category_id')),
        lower(json_extract_path_text(a.json_extensions, 'data.screenElementDetail[0].value'))) as json_val,
  
  --- determines if the json value is series or episode id
    case 
        when json_val in (select series_id from ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM) then 'series_id'
        when json_val in (select episode_id from ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM) then 'episode_id'
                                else 'neither' end as id_type,
  
  --- brings json value up to series id level
    case 
        when id_type = 'episode_id' then b.series_id
        when id_type = 'series_id' then json_val end as json_val_series
from    
    SANDBOX.ENGINEERING.UX_EVENT_FACT_US_VW3 a
LEFT JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM b
on
    json_val = b.episode_id
where
    ((a.screen_name in ('livecontentnow','livecontentlater','livechanneldetails','livefullscreen','livehome')
and  
    a.screen_element_name in('watchfromstart','watchnow')))

and 
    date_trunc('month',a.event_occurred_utc) = '2023-05-01'

group by 1,2,3,4,5,6,7,8,9
)

select
    a.client_id,
    a.session_id,
    a.event_name,
    a.screen_name,
    a.screen_element_name,
    a.event_occurred_utc,
    a.json_val_series,
    b.episode_id,
    b.ep_start,
    sum(b.tvms) as tvms,
    min(b.ep_start) as ep_start
from 
    linear_base a
join 

--- pulls in episode and series id for vod content watched in timeframe
    (
    select
        a.client_id,
        a.session_id,
        a.episode_id,
        b.series_id,
        min(a.video_segment_begin_utc) as ep_start,
        sum(a.total_viewing_minutes) as tvms
    from
        "ODIN_PRD"."RPT"."ALL_HOURLY_TVS_AGG" a
    LEFT JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM b
    on
        a.episode_id = b.episode_id
    where
        a.country = 'US'
    and 
        date_trunc('month',a.video_segment_begin_utc) = '2023-05-01'
    and
        a.channel_id = 'vod'
    group by
        1,2,3,4) b
--- joins on client, session, series id of episode watched and ep start occurring after watchfromstart/watchnow action from linear side
on    
    a.client_id = b.client_id
and
    a.session_id = b.session_id
and 
    a.json_val_series = b.series_id
and
    date_trunc('second',a.event_occurred_utc) <= date_trunc('second',b.ep_start)
  group by 1,2,3,4,5,6,7,8,9
limit 50  