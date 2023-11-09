
USE DATABASE SANDBOX;
USE SCHEMA ANALYSIS_PRODUCT;
//CREATE OR REPLACE TABLE IOS_REG_GM_120722 as
with base as 
(      select date_trunc('month', time_stamp)::date as date, c.app_name,  left(a.app_version,charindex('-',a.app_version)-1) as app_version, 
        round(left(a.app_version,charindex('.',a.app_version)-1)*1000 +  substr(a.app_version, charindex('.',a.app_version)+1, charindex('.',a.app_version,3)-charindex('.',a.app_version)-1)*1,0) as version,
       first_time_seen_utc::date as first_seen_reg, --client_first_seen_utc::date as all_first_seen,
        client_id, user_sid--, sum(users) as MAU
      from "ODIN_PRD"."RPT"."BI_USER_ENGAGEMENT360" a
        join "ODIN_PRD"."DW_ODIN"."CMS_USER_DIM_VW" b on a.user_sid = b.cms_user_sid
        join "ODIN_PRD"."STG"."DEVICE_MAPPING" c on a.app_name = c.sub_app_name
      where  date_trunc('month', time_stamp) >= ('2022-11-01')
//            and a.app_name in ('ios', 'tvos','androidtv', 'googletv', 'firetv', 'androidmobile', 'chromebook', 'tivo', 'verizon', 'web', 'windows', 'viziowatchfree', 'lgwebos')
       and a.app_name = 'ios'
            and first_time_seen_utc is not null
            and a.country = 'US'
            group by 1,2,3,4,5,6,7)
            
, comb as 
// start pulling device deduped aggs for appropriate versions
//    , apple as   

// roku 5.23

    (
    (SELECT a.date, a.app_name, a.app_version, version, client_id, user_sid--, MAU
//    FROM base a 
     from base a
    WHERE a.app_name in ('ios', 'tvos') and version >= 5025
    group by 1,2,3,4,5,6)
    
UNION
//     , android as        
    (SELECT a.date, a.app_name, a.app_version, version, client_id, user_sid
    FROM base a 
    WHERE a.app_name in ('androidtv', 'googletv', 'firetv', 'androidmobile', 'chromebook', 'tivo', 'verizon') and version >= 5021
    group by 1,2,3,4,5,6)       
UNION
//     , web as
    (SELECT a.date, a.app_name, a.app_version, version, client_id, user_sid 
    FROM base a 
    WHERE a.app_name in ('web', 'windows') and version >= 6007
//        and utm_medium != 'embed'
    group by 1,2,3,4,5,6)            
UNION
//     , webtech as
    (SELECT a.date, a.app_name, a.app_version, version, client_id, user_sid
    FROM base a 
    WHERE a.app_name in ('viziowatchfree', 'lgwebos') and version  >= 6002
    group by 1,2,3,4,5,6)   )
    
    , ttl as     
    (select a.date, a.app_name, a.app_version, count(distinct client_id) as users, count(distinct user_sid) as reg_users
    from comb a 
        group by 1,2,3
        order by 2,1)
      , MAU as   
        (select date, app_name, app_version, (users) as MAU
      from "ODIN_PRD"."RPT"."KPI_MONTHLY"
      where country_name = 'United States'
        and date >= '2022-11-01'
        AND APP_NAME = 'IOS'
      group by 1,2,3, 4)
      , comb2 as   
    ( SELECT a.date, a.app_name, a.app_version, users, reg_users, mau
     from ttl a join MAU b on a.app_name = lower(b.app_name) and a.date = b.date and a.app_version = b.app_version
     group by 1,2,3,4, 5,6)
        
        
SELECT date, app_name, sum(users) as users, sum(reg_users) as reg_users, sum(MAU) as MAU
FROM comb2
group by 1,2
