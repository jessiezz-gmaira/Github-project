CREATE OR REPLACE TABLE vod_leading_indicators_linear_all_w_intervals_nn_2023_04_20 AS

with base as (
SELECT
  
--- gathers details for each user, including first seen date,first vod date, and tenure
    a.app_name,
    a.client_id,
    date_trunc('day',b.client_first_seen_utc) as client_first_seen_date,
    MAX(CASE WHEN a.channel_id = 'vod' THEN 1 else 0 END) AS vod_users,
    MIN(CASE WHEN a.channel_id = 'vod' THEN date_trunc('day',a.video_segment_begin_utc) END) as first_vod_date,
    MAX(CASE WHEN a.channel_id = 'vod' THEN date_trunc('day',a.video_segment_begin_utc) END) as last_vod_date,
    datediff('day',client_first_seen_date,first_vod_date) as tenure_days_on_first_vod_watch,

--- gathers viewing metrics in a users first 30 days
    sum(case when  datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <= 30 then a.total_viewing_minutes end) as tvm_30d,
    
    sum(case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=30 then a.total_viewing_minutes end) as linear_tvm_30d,
    count(distinct case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=30 then a.session_id end) as linear_session_freq_30d,
    count(distinct case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=30 then e.series_id end) as num_unique_linear_series_watched_30d,
    linear_tvm_30d/linear_session_freq_30d as linear_asd_30d,
    
    count(distinct case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=30 then a.session_id end) as all_session_freq_30d,
    count(distinct case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=30 then e.series_id end) as num_unique_all_series_watched_30d,
    tvm_30d/all_session_freq_30d as all_asd_30d,

    
--- gathers viewing metrics in a users first 7 days
    sum(case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=7 then a.total_viewing_minutes end) as tvm_7d,

    sum(case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=7 then a.total_viewing_minutes end) as linear_tvm_7d,
    count(distinct case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=7 then a.session_id end) as linear_session_freq_7d,
    count(distinct case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=7 then e.series_id end) as num_unique_linear_series_watched_7d,
    linear_tvm_7d/linear_session_freq_7d as linear_asd_7d,
    
    count(distinct case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=7 then a.session_id end) as all_session_freq_7d,
    count(distinct case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=7 then e.series_id end) as num_unique_all_series_watched_7d,
    tvm_7d/all_session_freq_7d as all_asd_7d,

    
--- gathers viewing metrics in a users first day
    sum(case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) = 0 then a.total_viewing_minutes end) as tvm_1d,

    sum(case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=1 then a.total_viewing_minutes end) as linear_tvm_1d,
    count(distinct case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=1 then a.session_id end) as linear_session_freq_1d,
    count(distinct case when a.channel_id != 'vod' and datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=1 then e.series_id end) as num_unique_linear_series_watched_1d,
    linear_tvm_1d/linear_session_freq_1d as linear_asd_1d,
    
    count(distinct case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=1 then a.session_id end) as all_session_freq_1d,
    count(distinct case when datediff('day',client_first_seen_date,date_trunc('day',a.video_segment_begin_utc)) <=1 then e.series_id end) as num_unique_all_series_watched_1d,
    tvm_1d/all_session_freq_1d as all_asd_1d

FROM
    ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG a
join
    "BI"."PLUTO_DW"."ALL_CLIENT_FIRST_SEEN_VW" b
on
    a.client_id = b.client_id
join
    "ODIN_PRD"."DW_ODIN"."CMS_EPISODE_DIM" e
on
    a.episode_id = e.episode_id
where
    a.country = 'US'
and
    a.app_name IN ('androidtv','androidmobile','roku','firetv')

--- filters to new users between september 15 and october 15
and
    date_trunc('day',b.client_first_seen_utc) between '2022-09-15' and '2022-10-15'
--- filters to two month window for all new users between september 15 and october 15
and
    date_trunc('day',a.video_segment_begin_utc) between '2022-09-15' and '2022-11-15'
and
    a.timeline_aligned_flag = 'TRUE'
and
    a.clip_window_aligned_flag = 'TRUE'
and
    a.geo_aligned_flag = 'TRUE'
and
    a.ep_sources_aligned_flag = 'TRUE'
GROUP BY 
    1,2,3
),

bi_base as (
select
    a.app_name,
    a.client_id,
    a.client_first_seen_date,
    a.first_vod_date,
    a.last_vod_date,
    a.tenure_days_on_first_vod_watch,
    a.tvm_30d,
    a.linear_tvm_30d,
    a.linear_session_freq_30d,
    a.num_unique_linear_series_watched_30d,
    a.linear_asd_30d,
    a.all_session_freq_30d,
    a.num_unique_all_series_watched_30d,
    a.all_asd_30d,
    a.tvm_7d,
    a.linear_tvm_7d,
    a.linear_session_freq_7d,
    a.num_unique_linear_series_watched_7d,
    a.linear_asd_7d,
    a.all_session_freq_7d,
    a.num_unique_all_series_watched_7d,
    a.all_asd_7d,
    a.tvm_1d,
    a.linear_tvm_1d,
    a.linear_session_freq_1d,
    a.num_unique_linear_series_watched_1d,
    a.linear_asd_1d,
    a.all_session_freq_1d,
    a.num_unique_all_series_watched_1d,
    a.all_asd_1d,
  
--- gathers user action metrics in a users first 30 days    
    sum(case when b.screen_element_name in ('livel2nav','livel2') and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=30 then 1 else 0  end) as linear_num_category_browse_actions_30d,
    sum(case when b.screen_element_name in ('seechanneldetails1','seechanneldetails2') and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=30 then 1 else 0  end) as num_channel_browse_actions_30d,
    sum(case when b.screen_element_name = 'favoritechannel'and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=30 then 1 else 0 end) as num_favchannel_30d,
    sum(case when b.screen_element_name = 'search'and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=30 then 1 else 0 end) as search_30d,

    
--- gathers user action metrics in a users first 7 days
    sum(case when b.screen_element_name in ('livel2nav','livel2') and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=7 then 1 else 0  end) as linear_num_category_browse_actions_7d,
    sum(case when b.screen_element_name in ('seechanneldetails1','seechanneldetails2') and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=7 then 1 else 0  end) as num_channel_browse_actions_7d,
    sum(case when b.screen_element_name = 'favoritechannel'and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=7 then 1 else 0 end) as num_favchannel_7d,
    sum(case when b.screen_element_name = 'search'and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=7 then 1 else 0 end) as search_7d,
    
--- gathers user action metrics in a users first day
    sum(case when b.screen_element_name in ('livel2nav','livel2') and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=1 then 1 else 0  end) as linear_num_category_browse_actions_1d,
    sum(case when b.screen_element_name in ('seechanneldetails1','seechanneldetails2') and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=1 then 1 else 0  end) as num_channel_browse_actions_1d,
    sum(case when b.screen_element_name = 'favoritechannel'and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=1 then 1 else 0 end) as num_favchannel_1d,
    sum(case when b.screen_element_name = 'search'and datediff('day',a.client_first_seen_date,date_trunc('day',b.time_stamp)) <=1 then 1 else 0 end) as search_1d
from
    base a
join
    "ODIN_PRD"."RPT"."BI_USER_ENGAGEMENT360" b
on
    a.client_id = b.client_id
  
--- filters to vod only users

where
    a.vod_users = 1
  
--- filters to user actions in a users first 30 days
and
    b.country = 'US'
and
    b.app_name IN ('androidtv','androidmobile','roku','firetv')
and
    date_trunc('day',b.time_stamp) between '2022-09-15' and '2022-11-15'
group by
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
)

select
    app_name,
    client_id,
    client_first_seen_date,
    first_vod_date,
    last_vod_date,
    tenure_days_on_first_vod_watch,
    tvm_30d,
    linear_tvm_30d,
    linear_session_freq_30d,
    num_unique_linear_series_watched_30d,
    linear_asd_30d,
    all_session_freq_30d,
    num_unique_all_series_watched_30d,
    all_asd_30d,
    tvm_7d,
    linear_tvm_7d,
    linear_session_freq_7d,
    num_unique_linear_series_watched_7d,
    linear_asd_7d,
    all_session_freq_7d,
    num_unique_all_series_watched_7d,
    all_asd_7d,
    tvm_1d,
    linear_tvm_1d,
    linear_session_freq_1d,
    num_unique_linear_series_watched_1d,
    linear_asd_1d,
    all_session_freq_1d,
    num_unique_all_series_watched_1d,
    all_asd_1d,
    linear_num_category_browse_actions_30d,
    num_channel_browse_actions_30d,
    num_favchannel_30d,
    search_30d,
    linear_num_category_browse_actions_7d,
    num_channel_browse_actions_7d,
    num_favchannel_7d,
    search_7d,
    linear_num_category_browse_actions_1d,
    num_channel_browse_actions_1d,
    num_favchannel_1d,
    search_1d
from
    bi_base
where
    tenure_days_on_first_vod_watch <= 30
group by
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42;