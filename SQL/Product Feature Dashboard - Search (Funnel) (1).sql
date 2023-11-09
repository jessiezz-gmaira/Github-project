with date_params as ( select (current_date-3)::date as start_date, (current_date-3)::date as end_date
  
//      select min(b.date) start_date, max(b.date) end_date
//      from odin_prd.dw_odin.date_dim a
//      join odin_prd.dw_odin.date_dim b on a.year = b.year
//                                      and a.week_of_year = b.week_of_year
//      where a.date = current_date-7

)

, search_used_sessions as (
  
       select session_id, country, min(time_stamp) min_time_stamp
       from odin_prd.rpt.bi_user_engagement360
       where event_name = 'userAction' 
        and screen_element_name = 'search'
        and screen_name in ('vodhome', 'livehome')
        and country = 'US'
        and time_stamp::date between (select start_date from date_params) and (select end_date from date_params)
       group by 1,2
) 


, sessions as (

        select a.client_id, a.time_stamp, a.session_id, a.event_name, a.screen_name, a.screen_element_name, a.country, case when lower(a.app_name) = 'airplay' then 'ios' else a.app_name end as app_name,
                event_name||'_'||screen_name||'_'||screen_element_name as event_name_categorized
        from odin_prd.rpt.bi_user_engagement360 a
        join search_used_sessions b on a.session_id = b.session_id 
                                    and a.country = b.country
                                    and a.time_stamp >= b.min_time_stamp
        where a.time_stamp::date between (select start_date from date_params)-1 and (select end_date from date_params)+1
          and event_name_categorized not in ('changePlaybackState_NA_NA',
                                            'episodeStart_NA_NA',
                                            'cmPodBegin_NA_NA',
                                            'cmImpression_NA_NA',
                                            'castRequest_NA_NA',
                                            'castSuccess_NA_NA',
                                            'cmError_NA_NA',
                                            'videoError_NA_NA',
                                            'channelGuideLoaded_NA_NA',
                                            'clipStart_NA_NA',
                                            'pageView_vodseriesdetails_NA',
                                            'pageView_livechanneldetails_NA',
                                            'userAction_vodseriesdetails_seevodseriesdetails',
                                            'channelGuideRequest_NA_NA',
                                            'userAction_livechanneldetails_unfavoritechannel',
                                            'pageView_vodmoviedetails_NA',
                                            'pageView_vodseasondetails_NA',
                                            'userAction_vodseriesdetails_pressokwatchlistactiontt',
                                            'sessionReset_NA_NA',
                                            'undefinedError_NA_NA',
                                            'appLaunch_NA_NA',
                                            'signOutSuccessful_NA_NA',
                                            'userAction_vodseriesdetails_removefromwatchlist',
                                             'userAction_searchresults_unfavoritechannel',
                                             'userAction_vodmoviedetails_pressokwatchlistactiontt',
                                             'pageView_webactivation_NA',
                                             'pageView_webactivationcomplete_NA',
                                             'pageView_activationpage_NA'
                                            )
          and screen_element_name not in ('removefromwatchlist','unfavoritechannel', 'pressokwatchlistactiontt','ayswcontinuewatching','turnonkidsmode','gotowatchlistactiontt')

        order by a.session_id desc, a.time_stamp asc
) 

, sessions_events_ordered as (

        select sessions.*,
              row_number() over (partition by session_id order by time_stamp asc) as rank,
              timediff( second, lag(time_stamp, 1) over (partition by session_id order by time_stamp asc), time_stamp ) as min_taken_from_previous
        from sessions
) 

, sessions_events_ordered_max_rank as (
  
        select *, 
              min(case when event_name = 'appBackgrounded' 
                        or (screen_name not in ('livechanneldetails','NA','searchempty','searchentry','searchresults','vodmoviedetails','vodseasondetails','vodseriesdetails','webactivation')
                            and event_name_categorized not in ('userAction_livehome_search','userAction_vodhome_search','userAction_webactivation_searchresulttile') )
                        then rank end) over (partition by session_id) min_rank,
              max(rank) over (partition by session_id) max_rank
        from sessions_events_ordered 
) 
//select * from sessions_events_ordered_max_rank limit 3000

, sessions_events_capped as (

        select *, 
                case when event_name_categorized in ('userAction_livehome_search', 'userAction_vodhome_search') then 'search click'
                     when event_name_categorized = 'pageView_searchentry_NA' then 'search entry pageview'
                     when event_name_categorized = 'pageView_searchresults_NA' then 'search results view'
                     when event_name = 'userAction' and screen_element_name = 'searchresulttile' then 'search results click'
                     when event_name_categorized = 'pageView_searchempty_NA' then 'search results empty'
                     when event_name = 'appBackgrounded' then 'exited from search'
                     when screen_name not in ('livechanneldetails','NA','searchempty','searchentry','searchresults','vodmoviedetails','vodseasondetails','vodseriesdetails') then 'exited from search'
                     when screen_element_name in ('watchnow', 'favoritechannel', 'addtowatchlist','seevodepisode','suggestionlink',
                                                  'share','dismiss','seevodmoviedetails','search','continuewatching') then screen_element_name
                    else event_name_categorized 
                end as event_name_categorized_trimmed

        from sessions_events_ordered_max_rank 
        where rank <= nvl(min_rank, max_rank)
) 


, sessions_rolled as (
  
        select  min(time_stamp)::date date
                , client_id
                , session_id
                , app_name
                , country
                , timediff( second, min(time_stamp), max(time_stamp) ) as search_use_duration_sec
  
                , max(case when rank = 1 then event_name_categorized_trimmed end) funnel_1
                , max(case when rank = 2 then event_name_categorized_trimmed end) funnel_2
                , max(case when rank = 3 then event_name_categorized_trimmed end) funnel_3
                , max(case when rank = 4 then event_name_categorized_trimmed end) funnel_4
                , max(case when rank = 5 then event_name_categorized_trimmed end) funnel_5
                , max(case when rank = 6 then event_name_categorized_trimmed end) funnel_6
                , max(time_stamp) max_rank_time
                , min(time_stamp) min_rank_time
                
                , sum(case when event_name = 'impressionNonAd' and screen_element_name = 'searchtooltip' then 1 else 0 end) as impressions_search_tooltip
                , sum(case when event_name = 'userAction' and screen_element_name = 'search' then 1 else 0 end) as clicks_search
                , sum(case when event_name = 'userAction' and screen_element_name = 'searchresulttile' and screen_name = 'searchresults' then 1 else 0 end) as clicks_search_resulttitle
                , sum(case when event_name = 'userAction' and screen_element_name = 'suggestionlink' then 1 else 0 end) as clicks_suggestion_link
                , sum(case when event_name = 'userAction' and screen_element_name = 'sort' then 1 else 0 end) as clicks_sort
                , sum(case when event_name = 'userAction' and screen_element_name = 'settings' then 1 else 0 end) as clicks_settings
                , sum(case when event_name = 'userAction' and screen_element_name = 'watchnow' then 1 else 0 end) as clicks_watchnow
                , sum(case when event_name = 'userAction' and screen_element_name = 'addtowatchlist' then 1 else 0 end) as clicks_addtowatchlist
                , sum(case when event_name = 'userAction' and screen_element_name = 'continuewatching' then 1 else 0 end) as clicks_continuewatching
                , sum(case when event_name = 'userAction' and screen_element_name = 'favoritechannel' then 1 else 0 end) as clicks_favorite_channel
                , sum(case when event_name = 'pageView' and screen_name = 'searchentry' then 1 else 0 end) as pageviews_search_entry
                , sum(case when event_name = 'pageView' and screen_name = 'searchresults' then 1 else 0 end) as pageviews_search_results
                , sum(case when event_name = 'pageView' and screen_name = 'searchempty' then 1 else 0 end) as pageviews_search_empty

        from sessions_events_capped
        group by 2,3,4,5
)  

       
select   date
        , app_name
        , country
        , funnel_1
        , funnel_2
        , funnel_3
        , funnel_4
        , funnel_5
        , funnel_6
        
        , count(distinct client_id) as users
        , count(distinct session_id) as sessions
        , count(distinct case when clicks_search_resulttitle > 0 then session_id end) search_results_title_clicked_session
        
        , sum(search_use_duration_sec) search_use_duration_sec
        , sum(clicks_search) clicks_search
        , sum(clicks_search_resulttitle) clicks_search_resulttitle
        , sum(clicks_sort) clicks_sort
        , sum(clicks_suggestion_link) clicks_suggestion_link
        , sum(clicks_settings) clicks_settings

        , sum(impressions_search_tooltip) impressions_search_tooltip
        
        , sum(pageviews_search_entry) pageviews_search_entry
        , sum(pageviews_search_results) pageviews_search_results
        , sum(pageviews_search_empty) pageviews_search_empty

        , sum(clicks_watchnow) clicks_watchnow
        , sum(clicks_addtowatchlist) clicks_addtowatchlist
        , sum(clicks_continuewatching) clicks_continuewatching
        , sum(clicks_favorite_channel) clicks_favorite_channel
        
from sessions_rolled
where funnel_1 = 'search click'
group by 1,2,3,4,5,6,7,8,9