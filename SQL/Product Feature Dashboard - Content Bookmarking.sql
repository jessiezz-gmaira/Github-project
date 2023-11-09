with date_params as ( --select '2022-03-01'::date as start_date, '2022-03-31'::date as end_date
  
      select min(date) start_date
            , max(date) end_date
      from odin_prd.dw_odin.date_dim
      where year = year(dateadd('month', -1, current_date))
        and month = month(dateadd('month', -1, current_date))
  
), 


session_tvms as (
  
        select 
  
                date_trunc('month', hr.utc)::date as report_month,
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
          and ( (geo_aligned_flag = TRUE and timeline_aligned_flag = TRUE and ep_sources_aligned_flag = TRUE) or lower(dm.platform) = 'web' )
        group by 1,2,3,4
),


feat_events as (
  
        select
  
                lower(client_id) client_id
                , date_trunc('month',time_stamp)::date report_month
                , app_name
                , country
                , sum(case when screen_element_name = 'addtowatchlist' then 1 else 0 end) as addtowatchlist
                , sum(case when screen_element_name = 'favoritechannel' then 1 else 0 end) as favoritechannel
                , sum(case when screen_element_name = 'continuewatching' then 1 else 0 end) as continuewatching
                , sum(case when screen_element_name = 'watchnow' then 1 else 0 end) as watchnow
                
  
        
        from odin_prd.rpt.bi_user_engagement360 a
        where a.event_name = 'userAction' 
          and a.screen_element_name in ('addtowatchlist','favoritechannel','continuewatching','watchnow')
          and lower(a.country) = 'us'
          and a.time_stamp::date between (select start_date from date_params) and (select end_date from date_params)
        group by 1,2,3,4
),


features_exposed_devices as (

        select report_month, app_name, country, sum(addtowatchlist) addtowatchlist, sum(favoritechannel) favoritechannel, sum(continuewatching) continuewatching, sum(watchnow) watchnow from feat_events group by 1,2,3

),


features_users as (


        select distinct
  
               report_month
               , client_id
               , case when addtowatchlist > 0 then 1 else 0 end as flag_addtowatchlist
               , case when favoritechannel > 0 then 1 else 0 end as flag_favoritechannel
               , case when continuewatching > 0 then 1 else 0 end as flag_continuewatching
               , case when watchnow > 0 then 1 else 0 end as flag_watchnow
               , case when (addtowatchlist > 0 or favoritechannel > 0 or continuewatching > 0) then 1 else 0 end as flag_any
  
        from feat_events
)


select

        'monthly' as report_rollup_level,
        a.report_month,
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
        nvl(b.flag_addtowatchlist,0) flag_addtowatchlist,
        nvl(b.flag_favoritechannel,0) flag_favoritechannel,
        nvl(b.flag_continuewatching,0) flag_continuewatching,
        nvl(b.flag_watchnow,0) flag_watchnow,
        nvl(b.flag_any,0) flag_any,
        nvl(d.addtowatchlist,FALSE) as flag_feature_exposed_device_addtowatchlist,
        nvl(d.favoritechannel,FALSE) as flag_feature_exposed_device_favoritechannel,
        nvl(d.continuewatching,FALSE) as flag_feature_exposed_device_continuewatching,
        nvl(d.watchnow,FALSE) as flag_feature_exposed_device_watchnow,
        nvl(d.addtowatchlist+d.favoritechannel+d.continuewatching+d.watchnow,FALSE)  flag_feature_exposed_device_any,
        
        sum(tvms) as tvms,
        count(distinct a.client_id) as users,
        sum(a.sessions) as sessions,

        sum(nvl(c.addtowatchlist,0)) addtowatchlist,
        sum(nvl(c.favoritechannel,0)) favorite_channel,
        sum(nvl(c.continuewatching,0)) continuewatching,
        sum(nvl(c.watchnow,0)) watchnow
        


from session_tvms a
left join odin_prd.stg.device_mapping dm on a.app_name = dm.sub_app_name
left join features_users b on a.client_id = b.client_id 
                          and a.report_month = b.report_month
left join feat_events c on a.client_id = c.client_id 
                        and a.report_month = c.report_month
                        and a.country = c.country
left join features_exposed_devices d on lower(a.app_name) = lower(d.app_name) 
                                    and lower(a.country) = lower(d.country)
                                    and a.report_month = d.report_month

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17