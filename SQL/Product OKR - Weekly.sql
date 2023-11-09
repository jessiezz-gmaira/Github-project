with dates as ( --select '2022-03-01'::date as start_date, '2022-03-31'::date as end_date
  
      select '2021-11-29' as start_date, '2021-12-05' as end_date 
//      select min(date) start_date, max(date) end_date
//      from odin_prd.dw_odin.date_dim
//      where year = year(dateadd('week', -1, current_date))
//        and week_of_year = week(dateadd('week', -1, current_date))
  
), 


mau as (

        select

            agg.client_id
            , date_trunc('week', hr.utc)::date as date
            , case when agg.app_name = 'tivo' then upper(dm.sub_app_name) else upper(dm.app_name) end as app_name
            , upper(dm.sub_app_name) sub_app_name
            , upper(dm.platform) platform
            , upper(dm.partner) partner
            , upper(agg.country) country
            , case when c.channel_name = 'VOD' then 'VOD' else 'Linear' end as content_type
            
            , max(case when contains(agg.app_version, '%') then split_part(agg.app_version,'%',1) else split_part(agg.app_version,'-',1) end) over (partition by agg.client_id, date) as app_version
            , max(case when agg.live_flag = TRUE then 1 else 0 end) as live_flag
            , sum(agg.total_viewing_minutes) as total_viewing_minutes

        from odin_prd.rpt.all_hourly_tvs_agg agg
        left join odin_prd.stg.device_mapping dm        on agg.app_name = dm.sub_app_name
        join odin_prd.dw_odin.hour_dim hr               on agg.hour_sid = hr.hour_sid
        left join odin_prd.dw_odin.channel_dim c        on agg.channel_id = c.channel_id

        where hr.utc::date between (select start_date from dates) and (select end_date from dates)
          and agg.geo_aligned_flag = TRUE
        group by 1,2,3,4,5,6,7,8,app_version

    )
    

, engagement_events as (
  
        select
  
                lower(client_id) client_id
                , lower(session_id) session_id
                , date_trunc('week',time_stamp)::date date
                , upper(dm.partner) partner
                , upper(dm.platform) platform
                , upper(dm.app_name) app_name
                , upper(dm.sub_app_name) sub_app_name
                
                , upper(country) country
                , case when event_name = 'pageView' then 'pageView_'||screen_name else screen_element_name end as screen_element_name
                , case when channel_name = 'VOD' then 'VOD' else 'Linear' end as content_type
                , max(case when contains(app_version, '%') then split_part(app_version,'%',1) else split_part(app_version,'-',1) end) over (partition by lower(client_id), date) as app_version
                , count(distinct time_stamp) counts
        
        from odin_prd.rpt.bi_user_engagement360 a
        left join odin_prd.stg.device_mapping dm on a.app_name = dm.sub_app_name
        where a.event_name in ('userAction', 'pageView', 'vodEpisodeWatch')
          and lower(a.country) = 'us'
          and a.time_stamp::date between (select start_date from dates) and (select end_date from dates)
        group by 1,2,3,4,5,6,7,8,9,10,app_version
    ) 
  

, mau_agg as (

        select

              'weekly' as reporting_cadence
              , agg.date
              , 'tvms' as metric
              , agg.partner
              , agg.platform
              , agg.app_name 
              , case when live_flag = 1 then 
                  case agg.sub_app_name when 'firetv' then upper('firetvlive')            
                                  when 'androidtv' then upper('androidtvlive')
                                  when 'androidtvtivo' then upper('androidtvlivetivo')
                                  when 'lgwebos' then upper('lgchannels')
                                  when 'androidtvverizon' then upper('androidtvliveverizon')
                                  when 'firetvverizon' then upper('firetvliveverizon')
                                  when 'googletv' then upper('googletvlive')
                                  else upper(agg.sub_app_name) end
                else upper(agg.sub_app_name) end as sub_app_name
              , agg.app_version
              , agg.country
              , 'n/a' as screen_element_name
              , agg.content_type
              , sum(total_viewing_minutes) metric_counts
              , count(distinct client_id) users

        from mau agg
        group by 1,2,3,4,5,6,7,8,9,10,11


)


, engagement_events_agg as (

          select 
  
                'weekly' as reporting_cadence
                , e.date
                , 'engagement_events' as metric
                , e.partner
                , e.platform
                , e.app_name
                , e.sub_app_name
                , e.app_version
                , e.country
                , e.screen_element_name
                , e.content_type
                , sum(counts) metric_counts
                , count(distinct client_id) users

          from engagement_events e
          where exists (select 1 from mau agg
                        where e.client_id = agg.client_id 
                          and e.date = agg.date
                          and e.sub_app_name = agg.sub_app_name
                          and e.country = agg.country
                          and e.content_type = agg.content_type)
          group by 1,2,3,4,5,6,7,8,9,10,11
  
)

select a.*
from engagement_events_agg a

union all

select *
from mau_agg

  