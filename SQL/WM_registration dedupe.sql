-- run this for all devices including web (will have to decide later if we want to exclude web)
-- exclude web from the table creation and ios and tvos
    -- inclde evrything in table creation but filter out ios, tvos, and web
-- dates: 4/27-5/31
-- may only include may
--
-- first look at MPUs for exclusive content
-- 1. MPUs for people that watched exclusive vs didnt
-- 2. since its popular content we could pull the non exclsive IDs vs the exclusive IDs -- i.e. compare gunsmoke viewership to exclusive content gunsmoke viewership
-- definitely need something on the content
-- need overall metrics up to me to decide that
-- will need it validated by christine so send it to her no later than tomorrow
-- this is like one slide in a  deck for next week -- looking for something interesting to include
    -- need to look at all metrics for an initial sneak peak
    --
-- TVMS in a logged in state is what everyone is after
--
////////////////// NEW TABLE//////////////////
-- this is the NON walmart users base
CREATE OR REPLACE TABLE SANDBOX.ANALYSIS_PRODUCT.REGI_USERS_GT_052223 as
WITH reg_base as (
    SELECT DISTINCT
        a.user_id,
        b.user_sid,
        bi.client_id,
        a.first_time_seen_utc as reg_date,
        bi.country,
        cfs.client_first_seen_utc as client_first_seen_utc
        -- max(case when lower(entitlements) like '%walmart%' then a.first_time_seen_utc else null end) as wm_reg_date
    FROM
        ODIN_PRD.DW_ODIN.CMS_USER_DIM_VW a
        JOIN ODIN_PRD.DW_ODIN.USER_DIM_VW b on lower(a.user_id) = lower(b.user_id)
        LEFT JOIN ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 bi on bi.user_sid = b.user_sid
                                                        and lower(bi.country) = 'us'
                                                        and lower(bi.app_name) not in ('web','ios','tvos')
                                                        and date_trunc('day',time_stamp) between '2023-04-27' and '2023-05-31'
        LEFT JOIN ODIN_PRD.RPT.ALL_CLIENT_FIRST_SEEN cfs on cfs.client_id = bi.client_id
    -- GROUP BY 1,2,3,4
)
, x1 as (
    SELECT
        client_id,
        min(reg_date) as first_reg_date
    FROM
        reg_base
    WHERE 1=1 --wm_reg_date is not null
        AND client_id not in (SELECT client_id from SANDBOX.ANALYSIS_PRODUCT.WALMART_REPORTING_BASE)
    GROUP BY 1
)
-- ,first_reg as (
    SELECT
        distinct
        r.client_id,
        -- date_trunc('day',video_segment_begin_utc) as day_date,
        r.reg_date,
        -- agg.app_name,
        r.user_sid,
        r.user_id,
        r.country,
        r.client_first_seen_utc,
        x1.first_reg_date,
        -- agg.session_id,
        -- sum(total_viewing_minutes) as TVMs

        -- r.*,
        -- x1.first_reg_date,
        -- date_trunc('day',video_segment_begin_utc) as day_date,
        -- agg.session_id,
        -- agg.app_name,
        -- sum(total_viewing_minutes) as TVMs
    FROM
        reg_base r
        JOIN x1 on x1.client_id = r.client_id and x1.first_reg_date = r.reg_date
        -- JOIN ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG agg on lower(r.client_id) = lower(agg.client_id)
        --                                             and date_trunc('month',agg.video_segment_begin_utc) = '2023-04-01'
        --                                             and agg.GEO_ALIGNED_FLAG = True
        --                                             and agg.EP_SOURCES_ALIGNED_FLAG = True
        --                                             and agg.TIMELINE_ALIGNED_FLAG = True
        --                                             and agg.country = 'US'
    -- GROUP BY 1,2,3,4,5,6,7,8,9,10
;
////////////////// NEW TABLE//////////////////
-- pulling everything together to get a lits of client_ids that are registered as well as walmart enttieled for some
CREATE OR REPLACE TABLE SANDBOX.ANALYSIS_PRODUCT.WALMART_AND_REGI_GT_052223 AS
WITH wm_base as (
    SELECT
        client_id,
        min(wmreg_date) as first_wmreg_date
    FROM
        SANDBOX.ANALYSIS_PRODUCT.WALMART_REPORTING_BASE
    GROUP BY 1
)
SELECT
    wrb.client_id,
    wrb.user_sid,
    cfs.client_first_seen_utc,
    wrb.reg_date,
    wrb.wmreg_date,
    'Walmart entiteled' as client_id_type
FROM
    SANDBOX.ANALYSIS_PRODUCT.WALMART_REPORTING_BASE wrb
    JOIN wm_base wb on wb.client_id = wrb.client_id and wb.first_wmreg_date = wrb.wmreg_date
    LEFT JOIN ODIN_PRD.RPT.ALL_CLIENT_FIRST_SEEN cfs on cfs.client_id = wrb.client_id

UNION

SELECT
    client_id,
    user_sid,
    client_first_seen_utc,
    reg_date,
    null as wmreg_date,
    'Registered only' as client_id_type
FROM
    SANDBOX.ANALYSIS_PRODUCT.REGI_USERS_GT_052223
;
////////////////// NEW TABLE//////////////////
-- pulling in necessary date from teh all hourly agg table. this will be the alst table and the table i pull from to create the aggregated values for each grouping of users
CREATE OR REPLACE TABLE SANDBOX.ANALYSIS_PRODUCT.WALMART_REGI_FULL_DATA_GT_052223 AS
SELECT
    ag.client_id,
    ag.session_id,
    ag.video_segment_begin_utc,
    wr.user_sid,
    ag.app_name,
    cfs.client_first_seen_utc,
    wr.reg_date,
    wr.wmreg_date,
    case when client_id_type is null then 'Non registered' else client_id_type end as client_id_type,
    ag.channel_id,
    ag.episode_id,
    ed.series_id,
    case when ed.series_id in
                ('64473d39c6f09e001b0a407c','6434cb5272e1ff001a51671d','64472c2d8cd0fd001a7a8a60','6434c73ea659cb001a6ce767','6434c1074f1ca3001af52518',
                '6434ba22559c07001aafbc5a','64471fb7c6f09e001b09c0d5','6447210772e1ff001aa6adae','6419eeeef295e6001ac683cc','6434aaa2fe0679001a592281') then 1 else 0 end as WM_exclusive_content,
    total_viewing_seconds as tvs
FROM
    ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG ag
    LEFT JOIN SANDBOX.ANALYSIS_PRODUCT.WALMART_AND_REGI_GT_052223 wr on wr.client_id = ag.client_id
    LEFT JOIN ODIN_PRD.RPT.ALL_CLIENT_FIRST_SEEN cfs on cfs.client_id = ag.client_id
    LEFT JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ed on ed.episode_id = ag.episode_id
WHERE 1=1
    AND date_trunc('day',ag.video_segment_begin_utc) between '2023-04-27' and '2023-05-31'
    AND ag.GEO_ALIGNED_FLAG = True
    AND ag.EP_SOURCES_ALIGNED_FLAG = True
    AND ag.TIMELINE_ALIGNED_FLAG = True
    AND ag.country = 'US'
    AND lower(agg.app_name) not in ('web','ios','tvos')
ORDER BY 1,2,3
;
