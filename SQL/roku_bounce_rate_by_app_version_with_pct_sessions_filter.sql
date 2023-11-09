with dates as (
      select '2023-08-01' as start_date, '2023-09-30' as end_date
)
, bi360 as (
    select distinct
        date_trunc('day',time_stamp)::date date
        , lower(a.session_id) session_id
        , split_part(app_version, '-',1) app_version
    from odin_prd.rpt.bi_user_engagement360 a
        left join odin_prd.stg.device_mapping dm on a.app_name = dm.sub_app_name
    where lower(a.country) = 'us'
        and a.time_stamp::date between (select start_date from dates) and (select end_date from dates)
        and lower(dm.app_name) = 'roku'
)
, actives as (
    select distinct
        date_trunc('day', hr.utc)::date date
        , lower(agg.session_id) session_id
        , split_part(app_version, '-',1) app_version
    from odin_prd.rpt.all_hourly_tvs_agg agg
        join odin_prd.dw_odin.hour_dim hr               on agg.hour_sid = hr.hour_sid
        left join odin_prd.stg.device_mapping dm        on agg.app_name = dm.sub_app_name
    where hr.utc::date between (select start_date from dates) and (select end_date from dates)
        and agg.geo_aligned_flag = TRUE
        and lower(agg.country) = 'us'
        and lower(dm.app_name) = 'roku'
)

, max_pct as (
select
    i.date
    , i.app_version
    , count(distinct i.session_id) sessions_bi360
    , count(distinct a.session_id) sessions_active
  
    --- gathers the total count of active sessions for a given day
    , sum(sessions_active) over (partition by i.date) as total_active_sessions
  
    --- calculates the percent of active sessions by app version for a given day
    , sessions_active/total_active_sessions as pct_sessions
    , 1 - (count(distinct a.session_id) / count(distinct i.session_id) ) as bounce_rate
from bi360 i
    left join actives a on i.session_id = a.session_id and i.date = a.date and i.app_version = a.app_version
group by 1,2
)

select
    date,
    app_version,
    sessions_bi360,
    sessions_active,
    total_active_sessions,
    pct_sessions,
    bounce_rate,
    
    --- pulls the max pct of active sessions by app version in the timeframe selected
    max(pct_sessions) over (partition by app_version) as max_pct_sessions
from
    max_pct
group by
    1,2,3,4,5,6,7
    
    --- filters out all app versions without at least one day with >= 20% of active sessions
qualify
    max_pct_sessions >=0.2
