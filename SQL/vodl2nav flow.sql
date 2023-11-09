set  feature = 'vodl2nav';

USE DATABASE SANDBOX;
USE SCHEMA ANALYSIS_PRODUCT;
CREATE OR REPLACE TABLE VODSR_GM_120622 as
select app_name, date_trunc('month', event_occurred_utc) as month, client_id, session_id 
from SANDBOX.ENGINEERING.UX_EVENT_FACT_US_VW3 a
JOIN ODIN_PRD.DW_ODIN.APP_DIM b on a.app_sid = b.app_sid
  where screen_element_name in ($feature) AND
    b.app_name in  ('androidmobile','androidmobileverizon','androidtv','androidtvtivo','androidtvverizon',
                 'comcastx1','contour','cricket','firetablet','firetv','firetvverizon','googletv',
                 'lgwebos','roku','samsungtizen','vizio','xboxone', 'ios', 'tvos')
                 and date_trunc('month', event_occurred_utc) in ('2022-07-01', '2022-08-01','2022-09-01')
                 group by 1,2,3,4;
                 
CREATE OR REPLACE TABLE VODSR_sample_GM_120622_roku  as
select a.app_name, a.month, a.client_id, a.session_id, event_occurred_utc, b.event_name, screen_name, screen_element_name,
channel_id, channel_name, channel_id_ub, channel_name_ub, 
series_id, series_name, series_id_ub, series_name_ub,
episode_id, episode_name, episode_id_ub, episode_name_ub,
position_id, label, feature_type_extension, json_extensions, cms_vodcategory_id as vodcategory_id,
category_id_ub, user_category_id_ub, user_category_name_ub, user_vodcategory_id_ub, user_vodcategory_name_ub
from VODSR_GM_120622 a
join  SANDBOX.ENGINEERING.UX_EVENT_FACT_US_VW3 b on a.session_id = b.session_id 
    and a.month = date_trunc('month', b.event_occurred_utc) 
left join ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_DIM c on c.cms_vodcategories_sid = b.vod_category_sid
where a.app_name in ('roku') and month >= '2022-07-01'
    and (b.event_name in ('vodEpisodeWatch', 'episodeStart', 'continuewatching') OR
         b.screen_element_name in ('seevodmoviedetails', 'seevodseriesdetails', 'watchnow', 'vodl2nav'))
         group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
         ;
//select * 
//from VODSR_sample_GM_100522_roku
//where session_id = '00026ab9-297e-11ed-976f-0242ac110003'

//select session_id, event_occurred_utc, screen_element_name, episode_id, episode_id_ub, label, series_id, series_id_ub, episodeub_lag, seriesUB_lag, browse, watched
//from VODSR_sample2_GM_100522_roku
//where screen_element_name = 'seevodseriesdetails'
//limit 100000
//;

CREATE OR REPLACE TABLE VODSR_sample2_GM_120622_roku  as
with base as
(select a.app_name, a.month, a.event_occurred_utc, a.client_id, a.session_id, a.event_name, a.screen_element_name, episode_id, episode_ID_UB, label, 
 series_id, series_id_ub, coalesce(vodcategory_id, category_id_ub, user_category_id_ub) as vodcategory_id_comb

from VODSR_sample_GM_120622_roku a
    left join ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_DIM b on a.VODcategory_id = coalesce(vodcategory_id, category_id_ub, user_category_id_ub)
where  a.app_name in ('roku') 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)

select *,
LAG(episode_ID_UB) IGNORE NULLS OVER (partition by session_id ORDER BY event_occurred_utc) as episodeUB_lag, 
LAG(series_id_ub) IGNORE NULLS OVER (partition by session_id ORDER BY event_occurred_utc) as seriesUB_lag, 
LAG(vodcategory_id_comb) IGNORE NULLS OVER (partition by session_id ORDER BY event_occurred_utc) as cat_lag,
LAG(label) IGNORE NULLS OVER (partition by session_id ORDER BY event_occurred_utc) as label_lag,
case when episode_id = episodeUB_lag then 1 
    when episodeUB_lag =label then 1 
    when series_id = seriesUB_lag then 1
    when seriesUB_lag = label then 1
    when label_lag = episode_id then 1
    else 0 end as watched,
case when ifnull(episode_id, '') <> episode_id_ub and episode_id_ub is not null then 1 
    when ifnull(series_id, '') <> series_id_ub and series_id_ub is not null then 1 else 0 end as browse
from base;


// create the flow patterns
CREATE OR REPLACE TABLE VODSR_flow_GM_120622_roku  as
select client_id, session_id, month, event_occurred_utc, coalesce(vodcategory_id_comb, cat_lag) as category, event_name, screen_element_name, browse, watched,
match_number, msq, cl from VODSR_sample2_GM_120622_roku
  match_recognize(
    partition by session_id
    order by event_occurred_utc
    measures
        match_number() as "MATCH_NUMBER",
        match_sequence_number() as msq,
        classifier() as cl
    all rows per match 
    pattern(feature{1} element* watch{0,1})
    define
        feature as screen_element_name = 'vodl2nav',
        element as screen_element_name IN ('seevodmoviedetails', 'seevodseriesdetails') ,
        watch as event_name IN ('vodEpisodeWatch', 'episodeStart') or screen_element_name in ('watchnow')
)
where app_name ='roku'
order by client_id, session_id, match_number, msq;

//----------------------------------------------------

with base as (
    select month::date as month, client_id, session_id, match_number, 
        case when cl = 'FEATURE' then 1 else 0 end as Feature,
        case when cl = 'ELEMENT' and browse = 1 then 1 else 0 end as Element, 
        case when cl = 'WATCH' and watched = 1 then 1 
            when screen_element_name = 'watchnow' then 1 else 0 end as Watch
    FROM VODSR_flow_GM_120622_roku
  where month::date = '2022-09-01'
    group by 1,2,3,4,5,6,7)

, condense as (
    select month, client_id, session_id, match_number, sum(feature) as feature, sum(element) as element, sum(watch) as watch,
    concat(sum(feature)::char, sum(element)::char, sum(watch)::char) as pattern
    //sum(browse) as browse, sum(watches) as watches
    from base
    group by 1,2,3,4
    //order by session_id, match_number;
      )
select month,
case when pattern = '101' then '100' else pattern end as pattern, 
count(distinct client_id) as users, count(distinct session_id) as sessions, count(*)
from condense
where substring(pattern,1,1) = '1'
group by 1,2
order by 2;

//list out side rail categories
with base as (
select month, coalesce(b.name, c.main_category_name, category) as category_name, 
  client_id, session_id, match_number, 
    case when cl = 'FEATURE' then 1 else 0 end as Feature,
    case when cl = 'ELEMENT' and browse = 1 then 1 else 0 end as Element, 
    case when cl = 'WATCH' and watched = 1 then 1 
        when screen_element_name = 'watchnow' then 1 else 0 end as Watch
from SANDBOX.ANALYSIS_PRODUCT.VODSR_flow_GM_120622_roku a
left join ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_DIM b on a.category = b.cms_vodcategory_id
left join ODIN_PRD.DW_ODIN.CMS_MAIN_CATEGORIES_DIM c on a.category = c.main_category_name
  where month = '2022-08-01'
group by 1,2,3,4,5,6,7,8)

, condense as (
select month, 
  case category_name
    when  'Watch List' then 'watchlist'
    when 'Continuar' then 'continue'
    else category_name end as category_name , 
  client_id, session_id, match_number, sum(feature) as feature, sum(element) as element, sum(watch) as watch,
concat(sum(feature)::char, sum(element)::char, sum(watch)::char) as pattern
//sum(browse) as browse, sum(watches) as watches
from base
group by 1,2,3,4,5
//order by session_id, match_number;
  )
select to_date(month) as month, category_name, pattern, count(distinct client_id) as users, count(distinct session_id) as sessions, count(*)
from condense
where substring(pattern,1,1) = '1'

group by 1,2,3
order by 4 desc;