With base as
-- Understand user history for a set of registered accounts and their related devices.
With base as
(SELECT client_id, user_id
FROM SANDBOX.ENGINEERING.CLIENT_USER_MAPPING_VW
)

SELECT a.client_id, a.user_id, first_time_client_user_pair_utc,  registration_client_ind

FROM  SANDBOX.ENGINEERING.CLIENT_USER_MAPPING_VW a
    JOIN base b on a.user_id = b.user_id
    
ORDER BY user_id, first_time_client_user_pair_utc;

----------------------------------------------------
-- Pull logged-in tvms and pctage

SELECT a.client_id, app_name, loggedin_status_flag, FIRST_TIME_CLIENT_USER_PAIR_UTC,
sum(total_viewing_seconds)/60 tvm

FROM SANDBOX.ENGINEERING.USER_VIDEO_SEGMENT_FACT_VW a
//  Limits to devices associated with registration only due to inner join.  Use left join if you want both registered and unregistered devices.
    JOIN SANDBOX.ENGINEERING.CLIENT_USER_MAPPING_VW b on a.client_id = b.client_id
    
WHERE country_code = 'US' and date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) = '2023-06-01'
    and loggedin_status_flag = 1
    and GEO_ALIGNED_FLAG = True
    and EP_SOURCES_ALIGNED_FLAG = True
    and TIMELINE_ALIGNED_FLAG = True
//    Limits to versions after remediation
    and Fixed_app_version = 1

group by all;

----------------------------------------------------
-- Pull logged-in tvms and pctage while removing built in app devices.
-- Registered devices only

SELECT 
date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) date,
c.app_name,
count(distinct a.client_id) as devices,
sum(case when loggedin_status_flag = 1 then total_viewing_seconds end)/60 as logged_in_tvms,
sum(total_viewing_seconds)/60 tvm, 
round(100*(div0(sum(case when loggedin_status_flag = 1 then total_viewing_seconds end), sum(total_viewing_seconds))),0) logged_in_pct

FROM SANDBOX.ENGINEERING.USER_VIDEO_SEGMENT_FACT_VW a
    JOIN SANDBOX.ENGINEERING.CLIENT_USER_MAPPING_VW b on a.client_id = b.client_id
    JOIN ODIN_PRD.STG.DEVICE_MAPPING c on a.app_name = c.sub_app_name
    
WHERE country_code = 'US' 
    and date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) between '2023-01-01' and '2023-07-01'
//   to remove built in versions that don't have reg/logged in 
    and (c.app_name in ('roku', 'hisense', 'samsung/orsay', 'comcastx1', 'androidmobile', 'playstation', 'contour', 'xboxone', 'comcastxclass' ) OR 
        sub_app_name in ('androidmobileverizon', 'firetvverizon', 'androidtvverizon', 'samsungtizen', 'lgwebos', 'googletv', 'firetv', 'androidtv'))
    and GEO_ALIGNED_FLAG = True
    and EP_SOURCES_ALIGNED_FLAG = True
    and TIMELINE_ALIGNED_FLAG = True
	and Fixed_app_version = 1
	
	GROUP BY all
	ORDER BY 1;
	
----------------------------------------------------
-- Pull logged-in tvms and pctage while removing built in app devices.
-- Both registered and unregistered devices

SELECT 

c.app_name,
date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) date,
count(distinct a.client_id) as devices
case w
sum(case when loggedin_status_flag = 1 then total_viewing_seconds end)/60 as logged_in_tvms,
sum(total_viewing_seconds)/60 tvm, 
round(100*(div0(sum(case when loggedin_status_flag = 1 then total_viewing_seconds end), sum(total_viewing_seconds))),2) logged_in_pct

FROM SANDBOX.ENGINEERING.USER_VIDEO_SEGMENT_FACT_VW a
    left JOIN SANDBOX.ENGINEERING.CLIENT_USER_MAPPING_VW b on a.client_id = b.client_id
    JOIN ODIN_PRD.STG.DEVICE_MAPPING c on a.app_name = c.sub_app_name
    
WHERE country_code = 'US' 
    and date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) between '2023-01-01' and '2023-07-01'
//    and c.app_name = 'roku'
    and (c.app_name in ('roku', 'hisense', 'samsung/orsay', 'comcastx1', 'androidmobile', 'playstation', 'contour', 'xboxone', 'comcastxclass' ) OR 
        sub_app_name in ('androidmobileverizon', 'firetvverizon', 'androidtvverizon', 'samsungtizen', 'lgwebos', 'googletv', 'firetv', 'androidtv'))
    and GEO_ALIGNED_FLAG = True
    and EP_SOURCES_ALIGNED_FLAG = True
    and TIMELINE_ALIGNED_FLAG = True

GROUP BY all
ORDER BY 1;


SELECT 
    c.app_name,
    date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) date,
    count(distinct a.client_id) as users,
    count(distinct case when b.client_id is not null then a.client_id end) as registered_users,
    sum(case when loggedin_status_flag = 1 then total_viewing_seconds end)/60 as logged_in_tvms,
    sum(total_viewing_seconds)/60 as tvm, 
    round(100*(div0(logged_in_tvms, tvm)),2) logged_in_pct

FROM SANDBOX.ENGINEERING.USER_VIDEO_SEGMENT_FACT_VW a
    left JOIN SANDBOX.ENGINEERING.CLIENT_USER_MAPPING_VW b on a.client_id = b.client_id
    JOIN ODIN_PRD.STG.DEVICE_MAPPING c on a.app_name = c.sub_app_name
    
WHERE country_code = 'US' 
    and date_trunc('month', VIDEO_SEGMENT_BEGIN_UTC) between '2023-01-01' and '2023-07-01'
//    and c.app_name = 'roku'
    and (c.app_name in ('roku', 'hisense', 'samsung/orsay', 'comcastx1', 'androidmobile', 'playstation', 'contour', 'xboxone', 'comcastxclass' ) OR 
        sub_app_name in ('androidmobileverizon', 'firetvverizon', 'androidtvverizon', 'samsungtizen', 'lgwebos', 'googletv', 'firetv', 'androidtv'))
    and GEO_ALIGNED_FLAG = True
    and EP_SOURCES_ALIGNED_FLAG = True
    and TIMELINE_ALIGNED_FLAG = True

GROUP BY 1,2
ORDER BY 1,2;