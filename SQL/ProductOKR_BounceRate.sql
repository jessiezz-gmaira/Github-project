with dates as ( 
  
      select min(date) start_date, max(date) end_date
      from odin_prd.dw_odin.date_dim
      where year = year(dateadd('week', -1, current_date))
        and week_of_year = week(dateadd('week', -1, current_date))
  
)

, bi360 as (
  
        select distinct
  
                date_trunc('day',time_stamp)::date date             
                , upper(dm.partner) partner
                , upper(dm.platform) platform
                , upper(dm.app_name) app_name
                , upper(dm.sub_app_name) sub_app_name
                , lower(a.session_id) session_id
                , case when contains(a.app_version, '%') then split_part(a.app_version,'%',1) else split_part(a.app_version,'-',1) end as app_version
                , upper(country) country
        
        from odin_prd.rpt.bi_user_engagement360 a
        left join odin_prd.stg.device_mapping dm on a.app_name = dm.sub_app_name
        where lower(a.country) = 'us' 
          and a.time_stamp::date between (select start_date from dates) and (select end_date from dates)
//          and a.event_name = 'appLaunch'


) 

, inactives as (
  
        
        select distinct
  
                date_trunc('day', hr.utc)::date date
                , upper(dm.partner) partner
                , upper(dm.platform) platform
                , upper(dm.app_name) app_name
                , upper(dm.sub_app_name) sub_app_name
                , lower(agg.session_id) session_id
                , case when contains(agg.app_version, '%') then split_part(agg.app_version,'%',1) else split_part(agg.app_version,'-',1) end as app_version
                , upper(country) country
  
        from odin_prd.rpt.hourly_inactive_tvs_agg agg
        join odin_prd.dw_odin.hour_dim hr           on agg.hour_sid = hr.hour_sid
        left join odin_prd.dw_odin.client_dim c     on agg.client_sid = c.client_sid
        left join odin_prd.dw_odin.app_dim a        on agg.app_sid = a.app_sid
        left join odin_prd.stg.device_mapping dm    on a.app_name = dm.sub_app_name
        left join odin_prd.dw_odin.geo_dim g        on g.geo_sid = agg.geo_sid
        where hr.utc::date between (select start_date from dates) and (select end_date from dates)
          and agg.geo_aligned_flag = TRUE
          and lower(g.country) = 'us'
          and a.app_name != 'web'


  
  union all
  
        select distinct
  
                date_trunc('day', hr.utc)::date date
                , upper(dm.partner) partner
                , upper(dm.platform) platform
                , upper(dm.app_name) app_name
                , upper(dm.sub_app_name) sub_app_name
                , lower(agg.session_id) session_id
                , case when contains(agg.app_version, '%') then split_part(agg.app_version,'%',1) else split_part(agg.app_version,'-',1) end as app_version
                , upper(country) country
  
        from odin_prd.rpt.s_hourly_inactive_tvs_agg agg
        join odin_prd.dw_odin.hour_dim hr           on agg.hour_sid = hr.hour_sid
        left join odin_prd.dw_odin.s_client_dim c     on agg.client_sid = c.client_sid
        left join odin_prd.dw_odin.s_app_dim a        on agg.app_sid = a.app_sid
        left join odin_prd.stg.device_mapping dm    on a.app_name = dm.sub_app_name
        left join odin_prd.dw_odin.s_geo_dim g        on g.geo_sid = agg.geo_sid
        where hr.utc::date between (select start_date from dates) and (select end_date from dates)
          and agg.geo_aligned_flag = TRUE
          and lower(g.country) = 'us'


)


, actives as (
  
        select distinct
                
                date_trunc('day', hr.utc)::date date
                , upper(dm.partner) partner
                , upper(dm.platform) platform
                , upper(dm.app_name) app_name
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
                , lower(agg.session_id) session_id
                , case when contains(agg.app_version, '%') then split_part(agg.app_version,'%',1) else split_part(agg.app_version,'-',1) end as app_version
                , upper(country) country
  
        from odin_prd.rpt.all_hourly_tvs_agg agg
        join odin_prd.dw_odin.hour_dim hr               on agg.hour_sid = hr.hour_sid
        left join odin_prd.stg.device_mapping dm        on agg.app_name = dm.sub_app_name
        where hr.utc::date between (select start_date from dates) and (select end_date from dates)
          and agg.geo_aligned_flag = TRUE
          and lower(agg.country) = 'us'
          and agg.app_name not in ('tvos', 'ios') 

  
  union all
  
        select distinct
                
                date_trunc('day', hr.utc)::date date
                , upper(dm.partner) partner
                , upper(dm.platform) platform
                , upper(dm.app_name) app_name
                , upper(dm.sub_app_name) sub_app_name
                , lower(agg.session_id) session_id
                , case when contains(agg.app_version, '%') then split_part(agg.app_version,'%',1) else split_part(agg.app_version,'-',1) end as app_version
                , upper(country) country
  
        from odin_prd.rpt.hourly_tvs_agg agg
        join odin_prd.dw_odin.hour_dim hr           on agg.hour_sid = hr.hour_sid
        left join odin_prd.dw_odin.client_dim c     on agg.client_sid = c.client_sid
        left join odin_prd.dw_odin.app_dim a        on agg.app_sid = a.app_sid
        left join odin_prd.stg.device_mapping dm    on a.app_name = dm.sub_app_name
        left join odin_prd.dw_odin.geo_dim g        on g.geo_sid = agg.geo_sid
        where hr.utc::date between (select start_date from dates) and (select end_date from dates)
          and agg.geo_aligned_flag = TRUE
          and lower(g.country) = 'us'
          and a.app_name in ('tvos','ios')
        

) 



select 

        i.date
        , i.partner
        , i.platform
        , i.app_name
        , i.sub_app_name
        , i.app_version
        , count(distinct i.session_id) sessions_bi360
        , count(distinct w.session_id) sessions_inactive
        , count(distinct a.session_id) sessions_active
from bi360 i
left join actives a on i.session_id = a.session_id and i.date = a.date
left join inactives w on i.session_id = w.session_id and i.date = w.date
group by 1,2,3,4,5,6
  
  
  
  
        