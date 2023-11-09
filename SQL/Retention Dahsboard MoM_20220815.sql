with dates as (

        select add_months(date_trunc('month',current_date),-1) as start_date, last_day(start_date) as end_date

    )


, previous_month_users as (

        select

            agg.client_id,
            date_trunc('month', hr.utc) as event_month,
            case when agg.app_name = 'tivo' then dm.sub_app_name else dm.app_name end as app_name,
            dm.sub_app_name,
            dm.platform,
            dm.partner,
            agg.country,
//            max(agg.app_version) as app_version,
            max(case when agg.live_flag = TRUE then 1 else 0 end) as live_flag,
            max(case when fs.client_first_watched_utc::date = hr.utc::date then 1 else 0 end) as new_user_flag,
            min(hr.utc)::date as min_session_date,
            count(distinct session_id) as sessions,
            sum(agg.total_viewing_minutes) as total_viewing_minutes

        from odin_prd.rpt.all_hourly_tvs_agg agg
        left join odin_prd.stg.device_mapping dm        on agg.app_name = dm.sub_app_name
        left join odin_prd.rpt.all_client_first_seen fs on fs.client_id = agg.client_id
        join odin_prd.dw_odin.hour_dim hr               on agg.hour_sid = hr.hour_sid

        where hr.utc::date between (select add_months(start_date,-1) from dates) and (select add_months(end_date,-1) from dates)
					and agg.geo_aligned_flag = TRUE
        group by 1,2,3,4,5,6,7

    ) 


, reporting_month_users as (

        select
            
            distinct client_id
        
        from odin_prd.rpt.all_hourly_tvs_agg agg
        join odin_prd.dw_odin.hour_dim hr on agg.hour_sid = hr.hour_sid
        where hr.utc::date between (select start_date from dates) and (select end_date from dates)
					and agg.geo_aligned_flag = TRUE
        group by 1

    )
    


select 
      
      'MoM Retention' as metric,
      a.event_month::date report_month,
      case when c.client_id is null then 'Not Returned Next Month' else 'Returned Next Month' end as user_bucket_mom,
      case when a.new_user_flag = 1 and  c.client_id is null and sessions = 1 then 'New Users: One and Done'
           when a.new_user_flag = 1                                           then 'New Users'
                                                                              else 'Returning Users' end as user_bucket_oneanddone,
      upper(a.app_name) as device,
      case when live_flag = 1 then 
              case a.sub_app_name when 'firetv' then upper('firetvlive')
                              when 'androidtv' then upper('androidtvlive')
                              when 'androidtvtivo' then upper('androidtvlivetivo')
                              when 'lgwebos' then upper('lgchannels')
                              when 'androidtvverizon' then upper('androidtvliveverizon')
                              when 'firetvverizon' then upper('firetvliveverizon')
                              when 'googletv' then upper('googletvlive')
                              else upper(a.sub_app_name) end
           else upper(a.sub_app_name) end as sub_device,
      upper(a.platform) as device_type,
      upper(a.partner) partner,
      a.country,
      b.region,
//      a.app_version, 
      case when a.live_flag = 1 then 'Built-In' else 'O&O' end as integration_type,
      count(distinct a.client_id) users,
      sum(a.sessions) sessions,
      sum(a.total_viewing_minutes) as total_viewing_minutes,
      current_timestamp as run_date

from previous_month_users a
join odin_prd.stg.device_country_mapping b on upper(a.country) = upper(b.country) 
                                          and upper(a.sub_app_name) = upper(b.sub_app_name)
left join reporting_month_users c on a.client_id = c.client_id
group by 1,2,3,4,5,6,7,8,9,10,11