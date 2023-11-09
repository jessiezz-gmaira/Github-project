with date_params as ( --select '2022-03-01'::date as start_date, '2022-04-30'::date as end_date
  
      select min(date) start_date
            , max(date) end_date
      from odin_prd.dw_odin.date_dim
      where year = year(dateadd('month', -1, current_date))
        and month = month(dateadd('month', -1, current_date))
  
), 


session_tvms as (
  
        select 
  
                date_trunc('month', hr.utc) as date,
                case when agg.app_name = 'tivo' then agg.sub_app_name else agg.app_name end as app_name,
                lower(agg.client_id) client_id,
                agg.country,
                count(distinct session_id) sessions,
                max(live_flag) live_flag,
                sum(total_viewing_seconds)/60 tvms

        from odin_prd.rpt.all_hourly_tvs_agg agg
        join odin_prd.dw_odin.hour_dim hr on agg.hour_sid = hr.hour_sid
        left join odin_prd.stg.device_mapping dm on agg.app_name = dm.sub_app_name

        where  date_trunc('day', hr.utc) between (select start_date from date_params) and (select end_date from date_params)
          and lower(country) = 'us'
          and geo_aligned_flag = TRUE
          and timeline_aligned_flag = TRUE
          and ep_sources_aligned_flag = TRUE

        group by 1,2,3,4
),


feat_events as (
  
        select
  
                lower(client_id) client_id
                , date_trunc('month',time_stamp) date
                , app_name
                , country
  
                , sum(case when event_name = 'userAction' and screen_element_name = 'search' then 1 else 0 end) as clicks_search
                , sum(case when event_name = 'userAction' and screen_element_name = 'searchresulttile' and screen_name = 'searchresults' then 1 else 0 end) as clicks_search_resulttitle

                , sum(case when event_name = 'userAction' and screen_element_name = 'watchnow' then 1 else 0 end) as clicks_watchnow
                , sum(case when event_name = 'userAction' and screen_element_name = 'addtowatchlist' then 1 else 0 end) as clicks_addtowatchlist
                , sum(case when event_name = 'userAction' and screen_element_name = 'continuewatching' then 1 else 0 end) as clicks_continuewatching
                , sum(case when event_name = 'userAction' and screen_element_name = 'favoritechannel' then 1 else 0 end) as clicks_favorite_channel

                , min(case when event_name = 'userAction' and screen_element_name = 'search' then time_stamp end) min_search_clicked_ts
                , max(case when event_name = 'userAction' and screen_element_name = 'search' then time_stamp end) max_search_clicked_ts
                , max(case when event_name = 'appBackgrounded' then time_stamp end) max_app_backgrounded_ts
  
        
        from odin_prd.rpt.bi_user_engagement360 a
        where event_name in ('userAction', 'appBackgrounded')
          and screen_element_name in ('search','addtowatchlist','continuewatching','favoritechannel','watchnow','searchresulttile')
          and date_trunc('day',time_stamp) between (select start_date from date_params) and (select end_date from date_params)
          and lower(country) = 'us'
        group by 1,2,3,4
),



search_users as (


        select distinct
  
               date
               , client_id
               , case when clicks_search > 0 then 1 else 0 end as flag_search_used
               , case when clicks_search_resulttitle > 0 then 1 else 0 end as flag_search_results_clicked 
  
        from feat_events
)



select

        'monthly' as report_rollup_level,
        a.date::date as date,
        upper(dm.partner) partener,
        upper(dm.platform) platform,
        upper(dm.app_name) app_name,
        upper(case when live_flag = TRUE 
                then case when dm.sub_app_name = 'firetv' THEN 'firetvlive'
                          when dm.sub_app_name = 'androidtv' THEN 'androidtvlive'
                          when dm.sub_app_name = 'androidtvtivo' THEN 'androidtvlivetivo'
                          when dm.sub_app_name = 'lgwebos' THEN 'lgchannels'
                          when dm.sub_app_name = 'androidtvverizon' THEN 'androidtvliveverizon'
                          when dm.sub_app_name = 'firetvverizon' THEN 'firetvliveverizon'
                          when dm.sub_app_name = 'googletv' THEN 'googletvlive'
                          else dm.sub_app_name end
              else dm.sub_app_name end) as sub_app_name,
        a.country,
        nvl(b.flag_search_used,0) flag_search_used,
        nvl(b.flag_search_results_clicked,0) flag_search_results_clicked,
        
        sum(tvms) as tvms,
        count(distinct a.client_id) as users,
        count(distinct case when clicks_search_resulttitle>0 then a.client_id end) search_results_title_clicked_user,
        sum(a.sessions) as sessions,

        sum(c.clicks_search) clicks_search,
        sum(c.clicks_search_resulttitle) clicks_search_resulttitle,

        sum(c.clicks_watchnow) clicks_watchnow,
        sum(c.clicks_addtowatchlist) clicks_addtowatchlist,
        sum(c.clicks_continuewatching) clicks_continuewatching,
        sum(c.clicks_favorite_channel) clicks_favorite_channel


from session_tvms a
left join odin_prd.stg.device_mapping dm on a.app_name = dm.sub_app_name
left join search_users b on a.client_id = b.client_id 
                        and a.date = b.date
left join feat_events c on a.client_id = c.client_id 
                        and a.date = c.date
                        and a.country = c.country

group by 1,2,3,4,5,6,7,8,9