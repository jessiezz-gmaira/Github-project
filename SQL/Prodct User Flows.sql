//USE DATABASE SANDBOX;
//USE SCHEMA ANALYSIS_PRODUCT;
drop table PATHWAY_GM_090722;
//

USE DATABASE SANDBOX;
USE SCHEMA ANALYSIS_PRODUCT;
CREATE OR REPLACE table PATHWAY_GM_090722 as 
with event as 
(select session_id 
from "SANDBOX"."ENGINEERING"."UX_EVENT_FACT_US_VW2"
where screen_element_name = 'seechanneldetails2'
    and date(event_occurred_utc) = '2022-07-07'
    limit 100)

//, base as (    
    select a.session_id, event_occurred_utc, event_name, screen_name, screen_element_name, channel_id, a.channel_name, ub_channel_name, 
  episode_id, a.episode_name, ub_episode_name, a.series_id, series_name, ub_series_name, json_extensions, 
      json_extract_path_text(json_extensions, 'data.screenElementDetail[0].name') as level1name, 
  json_extract_path_text(json_extensions, 'data.screenElementDetail[0].value') as level1value,
  json_extract_path_text(json_extensions, 'data.screenElementDetail[1].name') as level2name, 
  json_extract_path_text(json_extensions, 'data.screenElementDetail[1].value') as level2value,
  label, item_position_x, item_position_y, is_auto_play, position_id
  from "SANDBOX"."ENGINEERING"."UX_EVENT_FACT_US_VW2" a join event b on a.session_id = b.session_id
  left join "ODIN_PRD"."DW_ODIN"."CHANNEL_DIM" c on c.channel_sid = a.channel_sid
  left join "ODIN_PRD"."DW_ODIN"."EPISODE_DIM" d on d.episode_sid = a.episode_sid

  where event_name not in ('pageView', 'scrollVertical', 'clickPause', 'clickSkipRewind', 'clickSkipForward',
                          'clickScrubStop', 'clickScrubStart', 'clickPlay', 'clickPause', 'subtitleOn',
                          'subtitleOff', 'swipeVertical', 'swipeHorizontal', 'tiltScreen', 'clickVolume' )
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24;
//____________________________________
ALTER TABLE PATHWAY_GM_090722 add channel_name_ch varchar(100), episode_name_ep varchar(200), series_name_se varchar(200);

UPDATE PATHWAY_GM_090722
SET channel_name_ch = new.channel_name
FROM (
 SELECT level1value, channel_dim.channel_id, channel_dim.channel_name
    FROM PATHWAY_GM_090722 a
  left join ODIN_PRD.DW_ODIN.CHANNEL_DIM on a.level1value = channel_dim.channel_id
  where level1name =  'channel_id'
  ) new
  where PATHWAY_GM_090722.level1value = new.channel_id


//select * from PATHWAY_GM_090622 
//where channel_name_ch is not null

UPDATE PATHWAY_GM_090722
SET episode_name_ep = new.episode_name
FROM (
 SELECT level1value, episode_dim.episode_id, episode_dim.episode_name
    FROM PATHWAY_GM_090722 a
  left join ODIN_PRD.DW_ODIN.EPISODE_DIM on a.level1value = episode_dim.episode_id
  where level1name =  'episode_id'
  ) new
  where PATHWAY_GM_090722.level1value = new.episode_id;

UPDATE PATHWAY_GM_090722
SET series_name_se = new.series_name
FROM (
 SELECT level1value, series_dim.series_id, series_dim.series_name
    FROM PATHWAY_GM_090722 a
  left join ODIN_PRD.DW_ODIN.SERIES_DIM on a.level1value = series_dim.series_id
  where level1name =  'series_id'
  ) new
  where PATHWAY_GM_090722.level1value = new.series_id;

// **************some values are missing names
//select * from PATHWAY_GM_090622 
//where level1name =  'series_id'

//____________________________________
ALTER TABLE PATHWAY_GM_090722 add channelID_lab varchar(100), currentPlay varchar(25);

UPDATE PATHWAY_GM_090722
SET channelID_lab = new.channelID_lab,
    currentPlay = new.currentPlay
FROM (
 SELECT channel_name, label, 
    substring(label, position('=' in label)+1 , position('|' in label)-position('=' in label)-1) as channelID_lab ,
    substring(label, position('ing=' in label)+4) as currentPlay
    FROM PATHWAY_GM_090722 a
  where substring(label,1,3)='new'
  ) new
  where PATHWAY_GM_090722.label = new.label;


select * from PATHWAY_GM_090622 
where substring(label,1,3)='new'

ALTER TABLE PATHWAY_GM_090722 add channelName_lab varchar(100);

UPDATE PATHWAY_GM_090722
SET channelName_lab = new.channelName_lab
FROM (
 SELECT label, channelID_lab, channel_dim.channel_name as channelName_lab
    FROM PATHWAY_GM_090722 a
    left join ODIN_PRD.DW_ODIN.CHANNEL_DIM on a.channelID_lab = channel_dim.channel_id
  where substring(label,1,3)='new'
  ) new
  where PATHWAY_GM_090722.label = new.label;
  
//  ____________________________________
ALTER TABLE PATHWAY_GM_090722 add UB_user_cat_id varchar(200), UB_cat_id varchar(200);

UPDATE PATHWAY_GM_090722
SET UB_user_cat_id = new.UB_user_cat_id,
    UB_cat_id = new.UB_cat_id
FROM (
 SELECT json_extensions, 
     case when level1name = 'user_category_id' then level1value
        when level2name = 'user_category_id' then level2value else null end as UB_user_cat_id,   
     case when level1name = 'category_id' then level1value
        when level2name = 'category_id' then level2value else null end as UB_cat_id  
    FROM PATHWAY_GM_090722 
  ) new
  where PATHWAY_GM_090722.json_extensions = new.json_extensions;
//  ____________________________________

ALTER TABLE PATHWAY_GM_090722 add ub_channel_name_new varchar(200), ub_episode_name_new varchar(200), ub_series_name_new varchar(200);    


UPDATE PATHWAY_GM_090722
SET ub_channel_name_new = new.ub_channel_name_new,
    ub_episode_name_new = new.ub_episode_name_new,
    ub_series_name_new = new.ub_series_name_new
FROM (
 SELECT ub_channel_name, channel_name_CH, channelname_lab, coalesce(ub_channel_name, channel_name_CH, channelname_lab) as ub_channel_name_new,
  ub_episode_name, episode_name_EP, coalesce(ub_episode_name, episode_name_EP) as ub_episode_name_new,
  ub_series_name, series_name_SE, coalesce(ub_series_name, series_name_SE) as ub_series_name_new
    FROM PATHWAY_GM_090722 --where session_id = 'fd1c31cd-fdad-11ec-b2fa-0242ac110003'
  ) new;
//  ____________________________________


SELECT session_id, event_occurred_utc, screen_name, screen_element_name, 
channel_name, ub_channel_name_new,
case when channel_name <> ub_channel_name_new then 1 else 0 end channelBrowse,
episode_name, ub_episode_name_new,
case when episode_name <> ub_episode_name_new then 1 else 0 end episodeBrowse,
series_name , ub_series_name_new,
case when series_name <> ub_series_name_new then 1 else 0 end seriesBrowse, 
json_extensions, ub_episode_name, episode_name_ep, coalesce(ub_episode_name, episode_name_EP)  as test
FROM PATHWAY_GM_090722 
where screen_element_name = 'watchnow' --and session_id = 'fd1c31cd-fdad-11ec-b2fa-0242ac110003'
order by session_id, event_occurred_utc;

select event_occurred_utc, event_name, screen_name, screen_element_name,
channel_name, ub_channel_name_new,
episode_name, ub_episode_name_new,
series_name , ub_series_name_new,
json_extensions, label, level1name, level1name
from PATHWAY_GM_090722
where session_id = 'fd1c31cd-fdad-11ec-b2fa-0242ac110003'


select event_name, screen_name, screen_element_name, count(*)
from SANDBOX.ENGINEERING.UX_EVENT_FACT_US_VW2
where screen_element_name = 'watchnow'
group  by 1,2,3
