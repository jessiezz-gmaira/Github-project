CREATE OR REPLACE table SANDBOX.ANALYSIS_PRODUCT.VODSECTION_MATCHES_BASE_062223 as with funnel as (
select event_occurred_utc,client_id, session_id,channel,event_name,screen_name,screen_element_name,label,episode_id,series_id_comb,series_id_ub,seriesub_lag,episodeub_lag,label_lag,event_lag,screen_element_name_lag,event_lead,
  feature_type_extension, 
    json_extensions,
  coalesce(vod_category_id_comb, cat_lag) as category, browse, watched,
match_number, msq, cl 
  from SANDBOX.ANALYSIS_PRODUCT.VODSECTION_EVENTS_BASE_TEST_062223
  match_recognize(
    partition by session_id
    order by event_occurred_utc
    measures
        match_number() as "MATCH_NUMBER",
        match_sequence_number() as msq,
        classifier() as cl
    all rows per match 

    --- adjusted watch to have no limit, funnel start by section to have at least one with no limit, followed by n section or browse before watch event
    pattern(section{1,} (section|browse)* watch* finish{0,1})
    define        
    
        section as  ((event_name = 'sectionSelect' and label='vod')
            OR (event_name = 'pageView' AND screen_name in ('vodhome'))
                     
            OR (event_name = 'userAction' and screen_element_name in ('vodl2','vodl2nav','herocarouseldetails'))),
    
    
        browse as ((
                  screen_element_name in ('seevodmoviedetails', 'seevodseriesdetails', 'seevodcollection'))
          
                   
                                                                   ),
        --- reminder to look into: vodhome pageviews right before episode start, exclude from match consideration?
        --- to ensure that the watch events gathered are not linear, added stipuation that the channel must equal vod for the episodeStart following browse events
        watch as (event_name in('episodeStart') and channel = 'vod'),
        finish as (event_name = 'sectionSelect' and label = 'live')
)
order by client_id, session_id, match_number, msq

)
select 
    client_id, 
    session_id, 
    match_number, 
    msq,
    cl, 
    event_name,
    screen_name,
    screen_element_name,
    episode_id,
    label,
    browse as browsed,
    watched,
    feature_type_extension, 
    json_extensions,
	
	--- breaks out where users entered the vod section
    case when cl = 'SECTION' and event_name = 'sectionSelect' and label='vod' then 'section'
         when cl = 'SECTION' and event_name = 'pageView' AND screen_name in ('vodhome')then 'vodhome'
         when cl = 'SECTION' and event_name = 'userAction' and screen_element_name in ('vodl2','vodl2nav') then 'siderail'
         when cl = 'SECTION' and event_name = 'userAction' and screen_element_name in ('herocarouseldetails')  then 'carousel'
         when cl = 'BROWSE' and event_name = 'userAction' and screen_element_name in ('seevodcollection') then 'collection'
            end as section_breakout,
			
	--- gathers the json extensions for siderail events		
    case when cl = 'SECTION' and event_name = 'userAction' and screen_element_name in ('vodl2','vodl2nav') then json_extensions end as siderail_json_extensions,
	
	--- lags siderail json and section breakout, will be used later to get the most recent browse events breakout and any jsons
    lag(siderail_json_extensions) ignore nulls over (partition by session_id order by msq) as siderail_json_lag,
            lag(cl) ignore nulls over (partition by session_id order by msq) as cl_lag,
    lag(section_breakout) ignore nulls over (partition by session_id order by msq) as section_breakout_lag,
    ---case when cl = 'BROWSE' then section_breakout_lag else null end as browse_breakout,
    ---lag(browse_breakout) ignore nulls over (partition by session_id order by msq) as browse_breakout_lag,
    ---case when cl = 'BROWSE' then json_extensions else null end as browse_json_extensions,
    ---lag(browse_json_extensions) ignore nulls over (partition by session_id order by msq) as browse_json_lag,
    sum(case when cl = 'SECTION' then 1 else 0 end) as section,
    sum(case when cl = 'BROWSE' then 1 else 0 end) as browse,
    sum(case when cl = 'WATCH' then 1 else 0 end) as watch,
    sum(case when cl = 'FINISH' then 1 else 0 end) as finish,
        min(case when cl = 'SECTION' then event_occurred_utc  end) as section_ts,
        min(case when cl = 'BROWSE'  then event_occurred_utc  end) as browse_ts, 
        min(case when cl = 'WATCH'  then event_occurred_utc  end) as Watch_ts,
        min(case when cl = 'FINISH' then event_occurred_utc  end) as Finish_ts--,
        
    
    FROM funnel
    where session_id not in (select session_id from "SANDBOX"."ANALYSIS_PRODUCT"."USERCHANNELS_NN_062223" where first_channel = 'vod' group by 1)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    order by 3,4