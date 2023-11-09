CREATE OR REPLACE TABLE MONTH7_ROKU_GM_081522  as
        select
            agg.client_id,
            date_trunc('month', hr.utc) as event_month,
            case when agg.app_name = 'tivo' then dm.sub_app_name else dm.app_name end as app_name,
            dm.sub_app_name,
            dm.platform,
            dm.partner,
            agg.country,
            max(case when fs.client_first_watched_utc::date = hr.utc::date then 1 else 0 end) as new_user_flag,
            max(case when agg.live_flag = TRUE then 1 else 0 end) as live_flag,
            min(hr.utc)::date as min_session_date,
            count(distinct session_id) as sessions,
            sum(agg.total_viewing_minutes) as total_viewing_minutes

        from odin_prd.rpt.all_hourly_tvs_agg agg
        left join odin_prd.stg.device_mapping dm        on agg.app_name = dm.sub_app_name
        left join odin_prd.rpt.all_client_first_seen fs on fs.client_id = agg.client_id
        join odin_prd.dw_odin.hour_dim hr               on agg.hour_sid = hr.hour_sid

        where hr.utc::date between '2022-07-01' and '2022-07-31'
            and date_trunc('month', fs.client_first_watched_utc)::date = date_trunc('month', hr.utc)::date
            and date_trunc('year', fs.client_first_watched_utc)::date = date_trunc('year', hr.utc)::date
          and agg.geo_aligned_flag = TRUE
          and agg.app_name = 'roku'
          and agg.country = 'US'
        group by 1,2,3,4,5,6,7;
		
		with tvms as (
  SELECT
            agg.client_id
           ,session_id
           ,fs.client_first_watched_utc
           ,seg."SEGMENT K-MEANS PCA 6" as segment
           ,min(video_segment_begin_utc) as beginning_of_session  
           ,sum(total_viewing_minutes) as TVMs 
    FROM ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG        agg 
    JOIN SANDBOX.ANALYSIS_PRODUCT.SEGMENTATION_1_DATA_GT_080122 seg on seg.client_id = agg.client_id
    left join odin_prd.rpt.all_client_first_seen fs on fs.client_id = agg.client_id
    join odin_prd.dw_odin.hour_dim hr on agg.hour_sid = hr.hour_sid
    
    WHERE 1=1
      AND country = 'US'
      AND lower(agg.app_name) = 'roku'
      and fs.client_first_watched_utc::date = hr.utc::date
      AND date_trunc('day',video_segment_begin_utc)::date between '2022-04-01' and '2022-04-30' 
      AND GEO_ALIGNED_FLAG = True
      AND TIMELINE_ALIGNED_FLAG = True
      AND EP_SOURCES_ALIGNED_FLAG = True
    GROUP BY 1,2,3,4
)

select segment, 
CASE WHEN tvms <=5 then 'Under5'
WHEN tvms > 5 and tvms <=10 then 'From_5_10'
WHEN tvms > 10 and tvms <=15 then 'From_10_15'
WHEN tvms > 15 then 'Over_15' end as sess1_tvms,
  count(distinct client_id) as users
from tvms
GROUP BY 1, 2
ORDER BY 1,2
   