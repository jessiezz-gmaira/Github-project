
CREATE OR REPLACE TABLE  "SANDBOX"."ANALYSIS_PRODUCT".Walmart_Reporting_base as 
with base as 
(SELECT a.user_id, b.user_sid, a.first_time_seen_utc as reg_date, 
    app_name, client_id, screen_name, min(time_stamp) time_stamp, entitlements
FROM ODIN_PRD.DW_ODIN.CMS_USER_DIM_VW a
JOIN ODIN_PRD.DW_ODIN.USER_DIM_VW b on lower(a.user_id) = lower(b.user_id)
LEFT JOIN ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 x on b.user_sid = x.user_sid and date(x.time_stamp) >= '2023-04-27'
where entitlements like '%walmart%'
    and event_name = 'pageView'
    and screen_name = 'walmartsuccess'
//    and x.user_sid in ('16236800')
group by all
)

, clients as ( 
-- joining to the user360 table to gather the appropriate client_ids that are associated with these USER_SID accounts
    SELECT 
        x.app_name ,
        x.client_id, 
        x.user_sid,
        b.time_stamp as WMreg_date, 
        reg_date,
        row_number() over (partition by x.client_id order by WMreg_date) as rn,  -- gathering the first walmart signup so that we only look at those "first" users per client_id 
        max(case when x.screen_name = 'walmartsuccess' then 1 else 0 end ) as registrant

    FROM  
        base b
        LEFT JOIN ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 x on b.user_sid = x.user_sid and date(x.time_stamp) >= '2023-04-27'
    WHERE 1=1 
        AND date_trunc('day',WMreg_date) >= '2023-04-27' 
        AND date_trunc('day', x.time_stamp) >= '2023-04-27' 
        AND date(x.time_stamp) >= date_trunc('day',WMreg_date)
    GROUP BY 1,2,3,4,5--,6
    QUALIFY rn = 1 

)

//, last as 
(select 
    s.client_id,
    date_trunc('day',video_segment_begin_utc)::date as date, 
    s.reg_date, 
    s.WMreg_date, 
    d.app_name,
    s.app_name as sub_app_name,
    s.user_sid, 
    agg.country, 
    registrant,
    sum(total_viewing_seconds)/60 as tvms,
    case when date_trunc('day', s.reg_date) = date_trunc('day',video_segment_begin_utc) then 1 else 0 end New_reg,
    case when date_trunc('day', s.wmreg_date) = date_trunc('day',video_segment_begin_utc) then 1 else 0 end New_WM_reg
FROM 
    clients s 
     join "ODIN_PRD"."RPT"."ALL_HOURLY_TVS_AGG" agg on lower(s.client_id) = lower(agg.client_id)
                                                    and date_trunc('day',video_segment_begin_utc) >= '2023-04-27'
                                                    and GEO_ALIGNED_FLAG = True
                                                    and EP_SOURCES_ALIGNED_FLAG = True
                                                    and TIMELINE_ALIGNED_FLAG = True
                                                    and country = 'US'
     join "ODIN_PRD"."STG"."DEVICE_MAPPING" d on d.sub_app_name = s.app_name                                                   
WHERE  1=1
     and date_trunc('day',wmreg_date) <= date_trunc('day',video_segment_begin_utc)

GROUP BY 1,2,3,4,5,6,7,8,9,11,12,date_trunc('day',video_segment_begin_utc)

ORDER BY 1,2);

-------------------------------------------------------;

CREATE OR REPLACE TABLE  SANDBOX.ANALYSIS_PRODUCT.Walmart_Reporting_DAILY as 
with new as 
(select date, 
app_name, 
sub_app_name,
country, 
case when registrant = 1 and date_trunc('day',wmreg_date) = date then 1 else 0 end as new_activate,
case when registrant = 1 and date_trunc('day',reg_date) = date then 1 else 0 end as new_reg,
count(distinct client_id) as users
from  SANDBOX.ANALYSIS_PRODUCT.Walmart_Reporting_base 
where registrant = 1 and date_trunc('day',wmreg_date) = date
group by 1,2,3,4,5,6
order by 1,2)

, totals as 
(select date, 
app_name, 
sub_app_name,
country, 
count(distinct client_id) as users,
sum(tvms) as tvms
from  SANDBOX.ANALYSIS_PRODUCT.Walmart_Reporting_base 
group by 1,2,3,4
order by 1,2)

select t.date, 
case 
    when t.app_name in ('chromebook', 'windows') then 'web'
    when t.app_name = 'verizon' then 'androidmobile'
else t.app_name end as app_name, 
t.sub_app_name,
t.users, tvms, 
sum(case when new_activate = 1 then n.users end) as entitlements,
sum(case when new_reg = 1 then n.users end) as registrations
from totals t left join new n on t.date = n.date and t.app_name = n.app_name and t.sub_app_name = n.app_name
where t.date between '2023-04-27' and dateadd(day, -2, current_date)
group by 1,2,3,4,5
order by 2,1
;