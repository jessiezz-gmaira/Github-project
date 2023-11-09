create or replace table SANDBOX.ANALYSIS_PRODUCT.SC_FINAL_AGG as

with content as 
    (SELECT 
    date::date as date , 
    app_name, 
    content_type, 
    user_type, 
    //client_id,
    //binge,
    count(distinct proper_content_id) as content_cnt,
    sum(case when content_order = 1  then content_tvms end) as Discovery_tvms,
    sum(case when content_order > 1  then content_tvms end) as Continued_tvms,
    sum(case when binge = 1 then content_tvms end) as binge_tvms,
    sum(content_tvms) tvms

    FROM SANDBOX.ANALYSIS_PRODUCT.SC_USER_CONTENT_HISTORY

    WHERE date between '2022-01-01' and current_date()
    GROUP BY all
    //ORDER BY app_name, client_id, date
)

, daily as 
    (SELECT date
    , app_name
    , user_type
    , content_type
    , count(distinct client_id) as DAUs
    , sum(total_tvms)/DAUs as MPU
    , sum(days_since_last_visit) as days_last

    FROM SANDBOX.ANALYSIS_PRODUCT.SC_CLIENT_DAILY_VAL_GT_080823
    WHERE date between '2022-01-01' and current_date()
    group by all
    //order by day_date
)
, daily_TTL as 
    (SELECT date
    , app_name
    , user_type
    , 'TTL' as content_type
    , case when total_tvms >= 5 then 1 else 0 end success
    , count(distinct client_id) as DAUs
    , sum(total_tvms)/DAUs as MPU
    , sum(days_since_last_visit) as days_last

    FROM SANDBOX.ANALYSIS_PRODUCT.SC_CLIENT_DAILY_VAL_GT_080823
    WHERE date between '2022-01-01' and current_date()
    group by all
    //order by day_date
)


, dates as 
    (select distinct date
    from  "SANDBOX"."ANALYSIS_PRODUCT"."SC_CLIENT_DAILY_VAL_GT_080823"
    where date > '2023-01-01')

, client as
    (select client_id
     , app_name
     , dateadd(day, -tenure_days, date) client_app_start
    from "SANDBOX"."ANALYSIS_PRODUCT"."SC_CLIENT_DAILY_VAL_GT_080823"
    where user_type = 'new'  
    )

,  new as 
    (
     SELECT  
        c.date,
        a.app_name, 
        a.client_id,
        client_app_start,
        dateadd(day, 30, client_app_start) as maxdate,
        count(distinct a.date) as days, 
        sum(total_tvms)/60 tvh

     FROM SANDBOX.ANALYSIS_PRODUCT.SC_CLIENT_DAILY_VAL_GT_080823 a
        join client b on a.client_id = b.client_id
        join dates c on c.date >= client_app_start
            AND c.date <= dateadd(day, 30, client_app_start)
     Where tenure_days <7  
     group by all
    )

, newvod as
(
SELECT b.date
    , a.app_name
    , a.client_id
  , client_app_start
    , min(case when content_type = 'vod' then tenure_days
        else '100' end) time_to_vod

     FROM client a JOIN dates b 
        on b.date >= client_app_start AND b.date <= dateadd(day, 30, client_app_start)
     LEFT JOIN SANDBOX.ANALYSIS_PRODUCT.SC_CLIENT_DAILY_VAL_GT_080823 c 
                on a.client_id = c.client_id --and c.content_type = 'vod'  
GROUP BY ALL
//order by  client_id, date
)


, time_vod as 
(select
    a.* 
    , case when time_to_vod = 0 then 'Day 1'
        when time_to_vod between 1 and 6 then 'Week 1'
        when time_to_vod <= 30 then 'Month 1'
        when time_to_vod > 30 then 'Not in Month 1'
        else NULL end time_to_vod
from new a join newvod b
    on a.client_id = b.client_id

group by all)

, newagg as
    (
    SELECT
      date,
      app_name,
      NULL as content_type,
      time_to_vod,
      count(distinct client_id) as New_Users,
      sum(days) days,
      sum(tvh) tvh
      from time_vod
      group by all
    )


SELECT 
    a.date , 
    a.app_name, 
    a.content_type, 
    a.user_type, 
    NULL as success,
    content_cnt,
    discovery_tvms,
    binge_tvms,
    tvms,
    DAUs,
    days_last,
    null as time_to_vod,
    null New_Users,
    null as wk1_days,
    null as wk1_tvh

from content a
JOIN daily b on a.date = b.date AND
    a.app_name=b.app_name AND
    a.content_type = b.content_type AND
    a.user_type = b.user_type
    group by all
    
    UNION ALL

SELECT 
    a.date , 
    a.app_name, 
    b.content_type, 
    a.user_type, 
    success,
    content_cnt,
    discovery_tvms,
    binge_tvms,
    tvms,
    DAUs,
    days_last,
    null as time_to_vod,
    null New_Users,
    null as wk1_days,
    null as wk1_tvh

from content a
JOIN daily_TTL b on a.date = b.date AND
    a.app_name=b.app_name AND
//    a.content_type = b.content_type AND
    a.user_type = b.user_type
    group by all
    
    UNION ALL


SELECT 
    date , 
    app_name, 
    content_type, 
    'new' as user_type, 
    null as success,
    null content_cnt,
    null discovery_tvms,
    null binge_tvms,
    null tvms,
    null DAUs,
    null days_last,
    time_to_vod,
    New_Users,
    days as wk1_days,
    tvh as wk1_tvh

from newagg
group by all
;

select content_type
from SANDBOX.ANALYSIS_PRODUCT.SC_FINAL_AGG
group by all

;

select
app_name, 
day_date as date, 
user_type,
content_type, 
sum(ttff_Tvm) ttff_Tvm, 
sum(TTFF_SESSION_COUNTS) TTFF_SESSION_COUNTS,
div0(sum(deeplink_tvm), sum(DEEPLINK_SESSIONS_COUNTS)) as deeplink_tvms,
sum(total_tvm) total_tvms
from "SANDBOX"."ANALYSIS_PRODUCT"."SC_APP_DAILY_GT_VAL_081423"
where day_date = '2023-05-01'
group by all
;