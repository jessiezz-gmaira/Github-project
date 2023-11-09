------ selects all unique client ids that appeared in bi user engagement in feb 23, gathering the app name, client id, and a flag for registered users based on user sid and first seen date in reg table
------ joins to all hourly tvs agg by client id and session id to ensure that tvms will be added based on sessions that have/don't have user_sid
----gathers tvms, freq, asd per month by device
explain
with base as (
select
    a.user_sid,
    a.app_name,
    a.client_id,
    case when b.first_time_seen_utc is not null then 1 else 0 end as reg_user_flag,
    sum(c.total_viewing_minutes) as tvm,
    count(distinct c.session_id) as num_sessions,
    tvm/num_sessions as asd
from 
    "ODIN_PRD"."RPT"."BI_USER_ENGAGEMENT360" a 
left join  
    "ODIN_PRD"."DW_ODIN"."CMS_USER_DIM_VW" b 
on 
    a.user_sid = b.cms_user_sid 
join
    "ODIN_PRD"."RPT"."ALL_HOURLY_TVS_AGG" c
on
    a.client_id = c.client_id and a.session_id = c.session_id
where 
    date_trunc('month',a.time_stamp) = '2023-02-01'
and
    a.app_name in ('roku','firetv')
and
    date_trunc('month',c.video_segment_begin_utc) = '2023-02-01'
and
    c.app_name in ('roku','firetv')
group by 
    1,2,3,4
),

--- gathers percentile values per device across all users for tvm, asd, and frequency at the 5th,10th,20th,30th,40th,50th,60th,70th,80th,90th,95th, and 99th percentiles
percentiles_setup as (
select
    app_name,
    percentile_cont(0.05) within group (order by tvm) as tvm_percentile_05,
    percentile_cont(0.10) within group (order by tvm) as tvm_percentile_10,
    percentile_cont(0.20) within group (order by tvm) as tvm_percentile_20,
    percentile_cont(0.30) within group (order by tvm) as tvm_percentile_30,
    percentile_cont(0.40) within group (order by tvm) as tvm_percentile_40,
    percentile_cont(0.50) within group (order by tvm) as tvm_percentile_50,
    percentile_cont(0.60) within group (order by tvm) as tvm_percentile_60,
    percentile_cont(0.70) within group (order by tvm) as tvm_percentile_70,
    percentile_cont(0.80) within group (order by tvm) as tvm_percentile_80,
    percentile_cont(0.90) within group (order by tvm) as tvm_percentile_90,
    percentile_cont(0.95) within group (order by tvm) as tvm_percentile_95,
    percentile_cont(0.99) within group (order by tvm) as tvm_percentile_99,
    percentile_cont(0.05) within group (order by num_sessions) as num_sessions_percentile_05,
    percentile_cont(0.10) within group (order by num_sessions) as num_sessions_percentile_10,
    percentile_cont(0.20) within group (order by num_sessions) as num_sessions_percentile_20,
    percentile_cont(0.30) within group (order by num_sessions) as num_sessions_percentile_30,
    percentile_cont(0.40) within group (order by num_sessions) as num_sessions_percentile_40,
    percentile_cont(0.50) within group (order by num_sessions) as num_sessions_percentile_50,
    percentile_cont(0.60) within group (order by num_sessions) as num_sessions_percentile_60,
    percentile_cont(0.70) within group (order by num_sessions) as num_sessions_percentile_70,
    percentile_cont(0.80) within group (order by num_sessions) as num_sessions_percentile_80,
    percentile_cont(0.90) within group (order by num_sessions) as num_sessions_percentile_90,
    percentile_cont(0.95) within group (order by num_sessions) as num_sessions_percentile_95,
    percentile_cont(0.99) within group (order by num_sessions) as num_sessions_percentile_99,
    percentile_cont(0.05) within group (order by asd) as asd_percentile_05,
    percentile_cont(0.10) within group (order by asd) as asd_percentile_10,
    percentile_cont(0.20) within group (order by asd) as asd_percentile_20,
    percentile_cont(0.30) within group (order by asd) as asd_percentile_30,
    percentile_cont(0.40) within group (order by asd) as asd_percentile_40,
    percentile_cont(0.50) within group (order by asd) as asd_percentile_50,
    percentile_cont(0.60) within group (order by asd) as asd_percentile_60,
    percentile_cont(0.70) within group (order by asd) as asd_percentile_70,
    percentile_cont(0.80) within group (order by asd) as asd_percentile_80,
    percentile_cont(0.90) within group (order by asd) as asd_percentile_90,
    percentile_cont(0.95) within group (order by asd) as asd_percentile_95,
    percentile_cont(0.99) within group (order by asd) as asd_percentile_99
from
    base
group by
    1
),


--- assigns percentiles per client_id for tvms, asd, and frequency
user_percentiles as (
select
    a.user_sid,
    a.app_name,
    a.client_id,
    a.reg_user_flag,
    case 
        when a.tvm <= b.tvm_percentile_05 then '5'
        when a.tvm > b.tvm_percentile_05 and a.tvm <= b.tvm_percentile_10 then '10'
        when a.tvm > b.tvm_percentile_10 and a.tvm <= b.tvm_percentile_20 then '20'
        when a.tvm > b.tvm_percentile_20 and a.tvm <= b.tvm_percentile_30 then '30'
        when a.tvm > b.tvm_percentile_30 and a.tvm <= b.tvm_percentile_40 then '40'
        when a.tvm > b.tvm_percentile_40 and a.tvm <= b.tvm_percentile_50 then '50'
        when a.tvm > b.tvm_percentile_50 and a.tvm <= b.tvm_percentile_60 then '60'
        when a.tvm > b.tvm_percentile_60 and a.tvm <= b.tvm_percentile_70 then '70'
        when a.tvm > b.tvm_percentile_70 and a.tvm <= b.tvm_percentile_80 then '80'
        when a.tvm > b.tvm_percentile_80 and a.tvm <= b.tvm_percentile_90 then '90'
        when a.tvm > b.tvm_percentile_90 and a.tvm <= b.tvm_percentile_95 then '95'
        when a.tvm > b.tvm_percentile_95 and a.tvm <= b.tvm_percentile_99 then '99'
    end as tvm_percentile,
    
    case 
        when a.num_sessions <= b.tvm_percentile_05 then '5'
        when a.num_sessions > b.num_sessions_percentile_05 and a.num_sessions <= b.num_sessions_percentile_10 then '10'
        when a.num_sessions > b.num_sessions_percentile_10 and a.num_sessions <= b.num_sessions_percentile_20 then '20'
        when a.num_sessions > b.num_sessions_percentile_20 and a.num_sessions <= b.num_sessions_percentile_30 then '30'
        when a.num_sessions > b.num_sessions_percentile_30 and a.num_sessions <= b.num_sessions_percentile_40 then '40'
        when a.num_sessions > b.num_sessions_percentile_40 and a.num_sessions <= b.num_sessions_percentile_50 then '50'
        when a.num_sessions > b.num_sessions_percentile_50 and a.num_sessions <= b.num_sessions_percentile_60 then '60'
        when a.num_sessions > b.num_sessions_percentile_60 and a.num_sessions <= b.num_sessions_percentile_70 then '70'
        when a.num_sessions > b.num_sessions_percentile_70 and a.num_sessions <= b.num_sessions_percentile_80 then '80'
        when a.num_sessions > b.num_sessions_percentile_80 and a.num_sessions <= b.num_sessions_percentile_90 then '90'
        when a.num_sessions > b.num_sessions_percentile_90 and a.num_sessions <= b.num_sessions_percentile_95 then '95'
        when a.num_sessions > b.num_sessions_percentile_95 and a.num_sessions <= b.num_sessions_percentile_99 then '99'
    end as num_sessions_percentile,
    
    case 
        when a.asd <= b.asd_percentile_05 then '5'
        when a.asd > b.asd_percentile_05 and a.asd <= b.asd_percentile_10 then '10'
        when a.asd > b.asd_percentile_10 and a.asd <= b.asd_percentile_20 then '20'
        when a.asd > b.asd_percentile_20 and a.asd <= b.asd_percentile_30 then '30'
        when a.asd > b.asd_percentile_30 and a.asd <= b.asd_percentile_40 then '40'
        when a.asd > b.asd_percentile_40 and a.asd <= b.asd_percentile_50 then '50'
        when a.asd > b.asd_percentile_50 and a.asd <= b.asd_percentile_60 then '60'
        when a.asd > b.asd_percentile_60 and a.asd <= b.asd_percentile_70 then '70'
        when a.asd > b.asd_percentile_70 and a.asd <= b.asd_percentile_80 then '80'
        when a.asd > b.asd_percentile_80 and a.asd <= b.asd_percentile_90 then '90'
        when a.asd > b.asd_percentile_90 and a.asd <= b.asd_percentile_95 then '95'
        when a.asd > b.asd_percentile_95 and a.asd <= b.asd_percentile_99 then '99'
    end as asd_percentile
    
from
    base a
join
    percentiles_setup b
on
    a.app_name = b.app_name
group by
    1,2,3,4,5,6,7
)



--- gathers device, tvm percentile, and counts of registered and non-registered users per device
select
    app_name,
    tvm_percentile,
    count(distinct case when reg_user_flag = 0 then client_id end) as num_non_reg_users,
    count(distinct case when reg_user_flag = 1 then client_id end) as num_reg_users
from
    user_percentiles
group by
    1,2