USE DATABASE SANDBOX;
USE SCHEMA ANALYSIS_PRODUCT;

create or replace table "SANDBOX"."ANALYSIS_PRODUCT"."PATH_1UNION_GM_032923" as                         
	SELECT
    	ux.client_id
        ,ux.session_id
        ,ux.EVENT_OCCURRED_UTC as time_stamp
        ,ux.event_name
        ,ux.screen_name
        ,ux.screen_element_name
        ,ux.episode_id
        ,ux.clip_id
        ,ux.channel_id
        ,ux.series_id
        ,ux.episode_id_ub
        ,ux.channel_id_ub
        ,ux.series_id_ub
        ,ux.episode_name_ub
        ,ux.series_name_ub
        ,ux.channel_name_ub
        ,ux.utm_sid
        ,ux.json_extensions
        ,ux.label
    FROM
		SANDBOX.ENGINEERING.UX_EVENT_FACT_US_VW3 	 ux
        LEFT JOIN ODIN_PRD.DW_ODIN.APP_DIM 			 app on app.app_sid = ux.app_sid
    WHERE
    	-- will need to correct and expand this date range
		date_trunc('day',ux.EVENT_OCCURRED_UTC) = '2023-01-10'
    	-- will need to add other devices
        and lower(app.app_name) = 'firetv'

    UNION

    SELECT
    	bi.client_id
        ,bi.session_id
        ,bi.time_stamp as time_stamp
        ,bi.event_name
        ,bi.screen_name
        ,bi.screen_element_name
        ,bi.episode_id
        ,bi.clip_id
        ,bi.channel_id
        ,bi.series_id
        ,null as episode_id_ub
        ,null as channel_id_ub
        ,null as series_id_ub
        ,null as episode_name_ub
        ,null as series_name_ub
        ,null as channel_name_ub
        ,null as utm_sid
        ,null as json_extensions
        ,null as label
    FROM
    	ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 bi
    WHERE
    	-- only need cmpodbegin from biuser360 table
    	lower(bi.event_name) = 'cmpodbegin'
        -- will need to correct and expand this date range
		and date_trunc('day',bi.time_stamp) = '2023-01-10'
        -- will need to add other devices
        and lower(bi.app_name) = 'firetv'
        and lower(bi.country) = 'us';

create or replace table "SANDBOX"."ANALYSIS_PRODUCT"."PATH_TVM_GM_040423" as             
        select a.*, sum(total_viewing_minutes) tvm
        from "SANDBOX"."ANALYSIS_PRODUCT"."PATH_1UNION_GM_032923" a
        left join ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG b on a.session_id = b.session_id and a.clip_id = b.clip_id
            and time_stamp = video_segment_begin_utc
            
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        order by session_id, time_stamp
;
