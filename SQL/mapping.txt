    CREATE OR REPLACE table SANDBOX.ANALYSIS_PRODUCT.REG_MAPPING_CONDENSED as
    
    select client_key, client_id, min(FIRST_TIME_CLIENT_USER_PAIR_UTC) first_paired, REGISTRATION_CLIENT_IND 
    from "SANDBOX"."ENGINEERING"."CLIENT_USER_MAPPING_VW"
    group by all;
	
	
    SELECT date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC)::date month,  app_name, 
    case when first_paired <= last_day(date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC)) and 
        d.client_key is not null then 1 else 0 end as reg_ind,
    count(distinct a.client_key) as users,
    sum(TOTAL_VIEWING_SECONDS)/60 tvm,
    ifnull(sum(case when LOGGEDIN_STATUS_FLAG = TRUE then TOTAL_VIEWING_SECONDS end),0)/60 logged_in,
    div0(ifnull(sum(case when LOGGEDIN_STATUS_FLAG = TRUE then TOTAL_VIEWING_SECONDS end),0),
        sum(TOTAL_VIEWING_SECONDS))*100 as pct_IN
FROM "BI_TVM_UAT"."PLUTO_DW"."USER_VIDEO_SEGMENT_FACT_VW" a

    JOIN "BI_TVM_UAT"."PLUTO_DW"."GEO_DIM_VW" c on c.geo_key = a.geo_key
    LEFT JOIN SANDBOX.ANALYSIS_PRODUCT.REG_MAPPING_CONDENSED  d on a.client_key = d.client_key
where 1=1
    and country_code = 'US' and date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) ='2023-05-01'
    and FIXED_APP_VERSION_FEATURE_TYPE = 1
        and timeline_aligned_flag = 1
        and geo_aligned_flag = 1
        and ep_source_aligned_flag = 1    
        and app_name in ('tvos')
        
    GROUP BY ALL

    ORDER BY tvm desc
    ;