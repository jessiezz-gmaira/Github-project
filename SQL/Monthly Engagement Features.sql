
with dates as ( --select '2022-03-01'::date as start_date, '2022-03-31'::date as end_date
  
      select min(date) start_date
            , max(date) end_date
      from odin_prd.dw_odin.date_dim
      where year = year(dateadd('month', -1, current_date))
        and month = month(dateadd('month', -1, current_date))
  
), 


mau as (

        select

            agg.client_id
            , date_trunc('month', hr.utc)::date as date
            , case when agg.app_name = 'tivo' then upper(dm.sub_app_name) else upper(dm.app_name) end as app_name
            , upper(dm.sub_app_name) sub_app_name
            , upper(dm.platform) platform
            , upper(dm.partner) partner
            , upper(agg.country) country
            , case when c.channel_name = 'VOD' then 'VOD' else 'Linear' end as content_type
            
            , max(agg.app_version) as app_version
            , max(case when agg.live_flag = TRUE then 1 else 0 end) as live_flag
            , sum(agg.total_viewing_minutes) as total_viewing_minutes

        from odin_prd.rpt.all_hourly_tvs_agg agg
        left join odin_prd.stg.device_mapping dm        on agg.app_name = dm.sub_app_name
        join odin_prd.dw_odin.hour_dim hr               on agg.hour_sid = hr.hour_sid
        left join odin_prd.dw_odin.channel_dim c on agg.channel_id = c.channel_id

        where hr.utc::date between (select start_date from dates) and (select end_date from dates)
          and agg.geo_aligned_flag = TRUE
        group by 1,2,3,4,5,6,7,8

    ) 


, engagement_events as (
  
        select
  
                lower(client_id) client_id
                , lower(session_id) session_id
                , date_trunc('month',time_stamp)::date date
                , upper(dm.partner) partner
                , upper(dm.platform) platform
                , upper(dm.app_name) app_name
                , upper(dm.sub_app_name) sub_app_name
                , app_version
                , upper(country) country
                , case when event_name = 'pageView' then 'pageView_'||screen_name else screen_element_name end as screen_element_name
                , case when channel_name = 'VOD' then 'VOD' else 'Linear' end as content_type
                , count(distinct time_stamp) counts
        
        from odin_prd.rpt.bi_user_engagement360 a
        left join odin_prd.stg.device_mapping dm on a.app_name = dm.sub_app_name
        where a.event_name in ('userAction', 'pageView', 'vodEpisodeWatch')
          and lower(a.country) = 'us'
          and a.time_stamp::date between (select start_date from dates) and (select end_date from dates)
        group by 1,2,3,4,5,6,7,8,9,10,11
    ) 
  

, mau_agg as (

        select

              'monthly' as reporting_cadence
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
              , null as screen_element_name
              , agg.content_type
              , sum(total_viewing_minutes) metric_counts
              , count(distinct client_id) users

        from mau agg
        group by 1,2,3,4,5,6,7,8,9,10,11

      union all

        select

              'monthly' as reporting_cadence
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
              , null as screen_element_name
              , 'Total' as content_type
              , sum(total_viewing_minutes) metric_counts
              , count(distinct client_id) users

        from mau agg
        group by 1,2,3,4,5,6,7,8,9,10,11

)


, engagement_events_agg as (

          select 
  
                'monthly' as reporting_cadence
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
  
  union all
  
          select 
  
                'monthly' as reporting_cadence
                , e.date
                , 'engagement_events' as metric
                , e.partner
                , e.platform
                , e.app_name
                , e.sub_app_name
                , e.app_version
                , e.country
                , e.screen_element_name
                , 'Total' as content_type
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

  union all
  
          select 
  
                'monthly' as reporting_cadence
                , e.date
                , 'engagement_events' as metric
                , e.partner
                , e.platform
                , e.app_name
                , e.sub_app_name
                , e.app_version
                , e.country
                , 'Total' as screen_element_name
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
  
  union all
  
          select 
  
                'monthly' as reporting_cadence
                , e.date
                , 'engagement_events' as metric
                , e.partner
                , e.platform
                , e.app_name
                , e.sub_app_name
                , e.app_version
                , e.country
                , 'Total' as screen_element_name
                , 'Total' as content_type
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

select a.*, b.users as base_mau
from engagement_events_agg a
left join mau_agg b on a.date = b.date
                    and a.sub_app_name = b.sub_app_name
                    and a.app_version = b.app_version
                    and a.country = b.country
                    and a.content_type = b.content_type

union all

select *, users as base_mau
from mau_agg