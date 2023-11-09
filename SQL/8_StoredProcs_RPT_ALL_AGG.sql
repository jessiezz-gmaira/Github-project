CREATE OR REPLACE PROCEDURE RPT.LOAD_ALL_HOURLY_TVS_AGG (DAG_ID varchar(50), TASK_ID varchar(50), EXECUTION_DATE varchar(50))
    RETURNS VARCHAR(250)
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});

    var beginTransaction = snowflake.createStatement(
      {sqlText: "BEGIN"});

    var commitTransaction = snowflake.createStatement(
      {sqlText: "COMMIT"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to load data into the RPT.ALL_HOURLY_TVS_AGG table' FROM DUAL"}); 
                                     
     try {

        beginTransaction.execute();
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad1 = snowflake.createStatement(
          {sqlText:`SET START_HOUR_SID = (SELECT MIN(HOUR_SID) FROM ODIN_PRD.DW_ODIN.HOUR_DIM WHERE DATE_TRUNC('day', UTC) = (SELECT DATEADD('day', -7, CURRENT_DATE())))`});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`DELETE FROM RPT.ALL_HOURLY_TVS_AGG WHERE HOUR_SID >= $START_HOUR_SID`});
        etlLoad2.execute();
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`INSERT INTO RPT.ALL_HOURLY_TVS_AGG
                    SELECT HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, LIVE_FLAG, APP_VERSION,
                        COUNTRY, CITY, REGION, DMA_CODE, 
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, SUM(TOTAL_VIEWING_SECONDS)/60 TOTAL_VIEWING_MINUTES, SUB_APP_NAME

                    FROM 	
                    (
                    --T
                    SELECT HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        CASE WHEN APP_NAME = 'weblegacy' THEN 'web' ELSE APP_NAME END AS APP_NAME, LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, CITY, REGION, DMA_CODE, 
                        REQUEST_DATETIME_START_UTC VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, NULL AS SUB_APP_NAME
                    FROM HIST.RPT.T_HOURLY_TVS_AGG
                    WHERE APP_NAME IN ('androidtv','chromecast','firetv','hisense','ios','nowtv','oculus','playstation 3','playstation 4','samsungorsay','samsungtizen','skyticket',
                                      'sonytv','tvos','viziosmartcast','viziovia2.x','viziovia3.x','weblegacy')
                      AND HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    --T: androidmobile, firetablet, cricket: before 9/19
                    UNION ALL
                    SELECT T_HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, CITY, REGION, DMA_CODE, 
                        REQUEST_DATETIME_START_UTC VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, NULL AS SUB_APP_NAME
                    FROM HIST.RPT.T_HOURLY_TVS_AGG
                      JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON T_HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    WHERE APP_NAME IN ('androidmobile','firetablet')
                    AND DATE_TRUNC('month', UTC) < '2019-09-01'
                      AND T_HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20                 

                    UNION ALL

                    --SP active
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID,
                        CASE WHEN APP_NAME = 'vizio' THEN 'viziosmartcast'
                          WHEN APP_NAME = 'androidtvlivetivo' THEN 'androidtvtivo'
                          WHEN APP_NAME = 'firetvlive' THEN 'firetv'
                          WHEN APP_NAME = 'lgchannels' THEN 'lgwebos'
                          WHEN APP_NAME = 'androidtvliveverizon' THEN 'androidtvverizon'
                          WHEN APP_NAME = 'firetvliveverizon' THEN 'firetvverizon'
                          WHEN APP_NAME = 'googletvlive' THEN 'googletv'
                          WHEN APP_NAME = 'comcastx1' AND CLIENT_MODEL_NAME ILIKE '%hisense%xtv%' THEN 'xclasstv'
                          ELSE APP_NAME
                          END AS APP_NAME,
                        CASE WHEN APP_NAME = 'androidtvlivetivo' THEN TRUE
                          WHEN APP_NAME = 'firetvlive' THEN TRUE
                          WHEN APP_NAME = 'lgchannels' THEN TRUE
                          WHEN APP_NAME = 'androidtvliveverizon' THEN TRUE
                          WHEN APP_NAME = 'firetvliveverizon' THEN TRUE
                          WHEN APP_NAME = 'googletvlive' THEN TRUE
                          ELSE FALSE
                          END AS LIVE_FLAG,
                        APP_VERSION,
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG,
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS,
                        CASE WHEN APP_NAME = 'tivo' AND SUB_APP_NAME = 'na' AND CLIENT_USER_AGENT ILIKE '%TIVO%'
                          THEN 'tivo'
                          ELSE SUB_APP_NAME
                        END AS SUB_APP_NAME
                    FROM ODIN_PRD.RPT.HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE
                    WHERE APP_NAME IN ('catalyst', 'comcastx1', 'contour', 'facebook', 'tivo', 'vizio', 'xboxone', 'hisense', 'virginmedia', 'androidtvtivo', 'androidtvlivetivo', 
                    'lgchannels', 'lgwebos', 'stbverizon', 'androidmobileverizon', 'androidtvverizon', 'androidtvliveverizon', 'androidmobiletelcel', 'ps5','slingtv','oculus','androidcharterav',
                    'firetvliveverizon','firetvverizon','samsungdaily','samsungmobiletvplus','chromebook' , 'googletvlive','googletv','lifefitness','samsungtizen','roku','rokuuk','claroandroid','watchfreeplus','stbclaro','vidaahisense','androidmobilehuawei','marriott')
                      AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    UNION ALL

                    --SP active: androidmobile, cricket, firetablet: 9/19 after
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, FALSE AS LIVE_FLAG, APP_VERSION,
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, SUB_APP_NAME
                    FROM ODIN_PRD.RPT.HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('androidmobile', 'cricket', 'firetablet')
                      AND DATE_TRUNC('month', UTC) >= '2019-09-01'
                      AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20  

                    UNION ALL

                    --SP active, androidtv, androidtvlive, firetv, firetvlive 5.0 above
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        CASE  
                             WHEN APP_NAME = 'androidtv' AND CLIENT_DIM.CLIENT_USER_AGENT ILIKE '%BRAVIA%' THEN 'sonytv'
                             WHEN APP_NAME = 'androidtv' AND CLIENT_DIM.CLIENT_USER_AGENT ILIKE '%TIVO%4K%' THEN 'androidtvtivo'
                             WHEN APP_NAME = 'androidtvlive' AND CLIENT_DIM.CLIENT_USER_AGENT ILIKE '%TIVO%4K%' THEN 'androidtvtivo' 
                             WHEN APP_NAME = 'androidtvlive' THEN 'androidtv'
                             WHEN APP_NAME = 'firetvlive' THEN 'firetv'   
                        ELSE APP_NAME END AS APP_NAME,
                        CASE WHEN APP_NAME = 'androidtvlive' THEN TRUE 
                          WHEN APP_NAME = 'firetvlive' THEN TRUE
                          ELSE FALSE 
                          END AS LIVE_FLAG, 
                        APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION, DMA_CODE, 
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, SUB_APP_NAME
                    FROM ODIN_PRD.RPT.HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('androidtv', 'androidtvlive', 'firetv', 'firetvlive')
                    AND SUBSTRING(APP_VERSION,0,1) >= 5
                      AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    UNION ALL

                    --SP active: viziowatchfree, 3/20 and after
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, FALSE AS LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, SUB_APP_NAME
                    FROM ODIN_PRD.RPT.HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('viziowatchfree')
                    AND DATE_TRUNC('month', UTC) >= '2020-03-01'
                      AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    UNION ALL

                    --SP inactive: viziowatchfree, 2/20 and before
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, FALSE AS LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION_NAME REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, SUB_APP_NAME
                    FROM ODIN_PRD.RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.APP_DIM APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.CLIENT_DIM CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('viziowatchfree') 
                    AND DATE_TRUNC('month', UTC) <= '2020-02-01'
                      AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    UNION ALL

                    --S active
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, FALSE AS LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION_NAME REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, NULL AS SUB_APP_NAME
                    FROM ODIN_PRD.RPT.S_HOURLY_TVS_AGG HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.S_APP_DIM APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_CLIENT_DIM CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('desktop','windows') AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    UNION ALL

                    --S active: viziowatchfree, 3/20 and after
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, FALSE AS LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION_NAME REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, NULL AS SUB_APP_NAME
                    FROM ODIN_PRD.RPT.S_HOURLY_TVS_AGG HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.S_APP_DIM APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_CLIENT_DIM CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('viziowatchfree')
                    AND DATE_TRUNC('month', UTC) >= '2020-03-01' AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    UNION ALL

                    --S inactive
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, FALSE AS LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION_NAME REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, NULL AS SUB_APP_NAME
                    FROM ODIN_PRD.RPT.S_HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.S_APP_DIM APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_CLIENT_DIM CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('msn', 'web') AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20

                    UNION ALL

                    --S inactive: viziowatchfree, 2/20 and before
                    SELECT HOURLY_TVS_AGG.HOUR_SID, CLIENT_ID, SESSION_ID, CHANNEL_ID, EPISODE_ID, CLIP_ID, 
                        APP_NAME, FALSE AS LIVE_FLAG, APP_VERSION, 
                        UPPER(COUNTRY) COUNTRY, GEO_DIM.CITY, GEO_DIM.REGION_NAME REGION, DMA_CODE,
                        VIDEO_SEGMENT_BEGIN_UTC,
                        TIMELINE_ALIGNED_FLAG, CLIP_WINDOW_ALIGNED_FLAG, GEO_ALIGNED_FLAG, EP_SOURCES_ALIGNED_FLAG, 
                        SUM(TOTAL_VIEWING_SECONDS) TOTAL_VIEWING_SECONDS, NULL AS SUB_APP_NAME
                    FROM ODIN_PRD.RPT.S_HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.S_APP_DIM APP_DIM ON HOURLY_TVS_AGG.APP_SID = APP_DIM.APP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_CLIENT_DIM CLIENT_DIM ON HOURLY_TVS_AGG.CLIENT_SID = CLIENT_DIM.CLIENT_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    JOIN ODIN_PRD.DW_ODIN.S_GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID 
                    LEFT JOIN STG.ZIP_TO_DMA ON GEO_DIM.ZIP_CODE = ZIP_TO_DMA.POSTAL_CODE 
                    WHERE APP_NAME IN ('viziowatchfree')
                      AND DATE_TRUNC('month', UTC) <= '2020-02-01' AND HOURLY_TVS_AGG.HOUR_SID >= $START_HOUR_SID
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20                  

                    )

                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,21`});
        res = etlLoad3.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        commitTransaction.execute();

        return "Succeeded. ";}
        
      catch (err)  {
        var rollback = snowflake.execute( { sqlText: `ROLLBACK;`} );
        beginTransaction.execute();

        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
        commitTransaction.execute();
 
        return "Failed: " + err.message;}
    
    $$;

CREATE OR REPLACE PROCEDURE RPT.LOAD_CONTENT_PARTNER_DAILY (DAG_ID varchar(50), TASK_ID varchar(50), EXECUTION_DATE varchar(50))
    RETURNS VARCHAR(250)
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION SET TIMEZONE = 'America/New_York'"});

    var beginTransaction = snowflake.createStatement(
      {sqlText: "BEGIN"});

    var commitTransaction = snowflake.createStatement(
      {sqlText: "COMMIT"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to load data into the RPT.CONTENT_PARTNER_DAILY table' FROM DUAL"}); 
                                     
     try {

        beginTransaction.execute();
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad1 = snowflake.createStatement(
          {sqlText:`SET DAILY_START = (SELECT DATEADD('day', -92, CURRENT_DATE()));`});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`SET INC_DAILY_START = (SELECT DATEADD('day', -3, CURRENT_DATE()));`});
        etlLoad2.execute();
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`SET DAILY_END = (SELECT DATEADD('day', -2, CURRENT_DATE()));`});
        etlLoad3.execute();
        
        var etlLoad4 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE HIST.RPT.CONTENT_PARTNER_TEMP
                    AS
                    SELECT DATE_TRUNC('day',HOUR_DIM.EST)::DATE event_date_eastern, 
                      ALL_HOURLY_TVS_AGG.CLIENT_ID, 
                      ALL_HOURLY_TVS_AGG.session_id, 
                      ALL_HOURLY_TVS_AGG.COUNTRY user_country_id, 
                      CMS_EPISODE_DIM.episode_name, 
                      CMS_PARTNER_DIM.PARTNER_ID content_partner_ID,
                      CMS_PARTNER_DIM.PARTNER_NAME content_partner_name, 
                      IFNULL(PARENT_CHANNEL_MAPPING.parent_channel_name, CMS_CHANNEL_DIM.CHANNEL_NAME) parent_channel_name, 
                      IFNULL(PARENT_PARTNER_MAPPING.parent_partner_name, CMS_PARTNER_DIM.PARTNER_NAME) parent_partner_name,
                      CMS_SERIES_DIM.series_name, 
                      CMS_CLIP_DIM.clip_name, 
                      NULL tableau_emails, 
                      SUM(total_viewing_seconds)/60 total_viewing_minutes
                    FROM ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG
                    JOIN ODIN_PRD.DW_ODIN.HOUR_DIM ON ALL_HOURLY_TVS_AGG.HOUR_SID = HOUR_DIM.HOUR_SID
                    JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM ON ALL_HOURLY_TVS_AGG.EPISODE_ID = CMS_EPISODE_DIM.EPISODE_ID
                    JOIN ODIN_PRD.DW_ODIN.CMS_SERIES_DIM ON CMS_EPISODE_DIM.SERIES_ID = CMS_SERIES_DIM.SERIES_ID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_DIM ON ALL_HOURLY_TVS_AGG.CLIP_ID = CMS_CLIP_DIM.CLIP_ID
                    JOIN ODIN_PRD.DW_ODIN.CMS_PARTNER_DIM ON CMS_CLIP_DIM.PARTNER_ID = CMS_PARTNER_DIM.PARTNER_ID
                    JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM ON ALL_HOURLY_TVS_AGG.CHANNEL_ID = CMS_CHANNEL_DIM.CHANNEL_ID
                    LEFT JOIN ODIN_PRD.STG.PARENT_PARTNER_MAPPING ON CMS_PARTNER_DIM.PARTNER_ID = PARENT_PARTNER_MAPPING.PARTNER_ID
                    LEFT JOIN ODIN_PRD.STG.PARENT_CHANNEL_MAPPING ON CMS_CHANNEL_DIM.CHANNEL_ID = PARENT_CHANNEL_MAPPING.CHANNEL_ID
                    WHERE ALL_HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG = TRUE
                    AND EP_SOURCES_ALIGNED_FLAG = TRUE
                    AND ALL_HOURLY_TVS_AGG.GEO_ALIGNED_FLAG = TRUE
                    --AND CLIP_WINDOW_ALIGNED_FLAG = TRUE
                    AND IFNULL(PARENT_PARTNER_MAPPING.parent_partner_name, CMS_PARTNER_DIM.PARTNER_NAME) IS NOT NULL
                    AND ALL_HOURLY_TVS_AGG.COUNTRY IN (SELECT DECODE FROM ODIN_PRD.RPT.CODE_DECODE_MAPPING  WHERE CODE='CONTENT_PARTNER_DAILY_VALUES' )
                    AND (CMS_CHANNEL_DIM.CATEGORY_NAME <> 'Testing' OR CMS_CHANNEL_DIM.CHANNEL_NAME = 'VOD')
                    --AND APP_NAME IN ('androidmobile', 'androidtv', 'firetablet', 'cricket')
                    --AND SUBSTRING(APP_VERSION, 0, 1) >= 5
                    AND DATE_TRUNC('day',HOUR_DIM.EST)::DATE >= $DAILY_START
                    AND DATE_TRUNC('day',HOUR_DIM.EST)::DATE <= $DAILY_END
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12`});
        etlLoad4.execute();
        
        var etlLoad5 = snowflake.createStatement(
          {sqlText:`UPDATE HIST.RPT.CONTENT_PARTNER_TEMP
                    SET CONTENT_PARTNER_TEMP.TABLEAU_EMAILS = TABLEAU_EMAILS.EMAILS
                    FROM (SELECT PARTNER_ID, COUNTRY,
                    LISTAGG(LOWER(EMAIL), ', ') EMAILS
                    FROM ODIN_PRD.STG.TABLEAU_PARTNER_CONTACTS
                    GROUP BY 1,2) TABLEAU_EMAILS
                    WHERE CONTENT_PARTNER_TEMP.CONTENT_PARTNER_ID = TABLEAU_EMAILS.PARTNER_ID
                    AND CONTENT_PARTNER_TEMP.USER_COUNTRY_ID = TABLEAU_EMAILS.COUNTRY
                    AND CONTENT_PARTNER_TEMP.TABLEAU_EMAILS IS NULL`});
        etlLoad5.execute();
        
        var etlLoad6 = snowflake.createStatement(
          {sqlText:`DELETE FROM ODIN_PRD.RPT.CONTENT_PARTNER_DAILY WHERE event_date_eastern < $DAILY_START`});
        etlLoad6.execute();
        
        var etlLoad7 = snowflake.createStatement(
          {sqlText:`DELETE FROM ODIN_PRD.RPT.CONTENT_PARTNER_DAILY WHERE event_date_eastern >= $INC_DAILY_START`});
        etlLoad7.execute();
        
        var etlLoad8 = snowflake.createStatement(
          {sqlText:`INSERT INTO ODIN_PRD.RPT.CONTENT_PARTNER_DAILY
                    (event_date_eastern, CLIENT_ID, session_id, user_country_id, 
                    episode_name, content_partner_id, content_partner_name, 
                    parent_channel_name, parent_partner_name, series_name, clip_name, 
                    tableau_emails, total_viewing_minutes)
                    SELECT event_date_eastern, CLIENT_ID, session_id, user_country_id, 
                    episode_name, content_partner_id, content_partner_name, 
                    parent_channel_name, parent_partner_name, series_name, clip_name, 
                    tableau_emails, SUM(total_viewing_minutes) total_viewing_minutes
                    FROM HIST.RPT.CONTENT_PARTNER_TEMP
                    WHERE event_date_eastern BETWEEN $INC_DAILY_START AND $DAILY_END
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;`});
        res = etlLoad8.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();

        commitTransaction.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var rollback = snowflake.execute( { sqlText: `ROLLBACK;`} );
        beginTransaction.execute();

        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
        commitTransaction.execute();
 
        return "Failed: " + err.message;}
    
    $$;

CREATE OR REPLACE PROCEDURE RPT.LOAD_CONTENT_PARTNER_WEEKLY (DAG_ID varchar(50), TASK_ID varchar(50), EXECUTION_DATE varchar(50))
    RETURNS VARCHAR(250)
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION SET TIMEZONE = 'America/New_York'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to load data into the RPT.CONTENT_PARTNER_WEEKLY table' FROM DUAL"}); 
                                     
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad1 = snowflake.createStatement(
          {sqlText:`SET WEEKLY_START = (SELECT DATEADD('week', -1, DATE_TRUNC('week', CURRENT_DATE())));`});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`DELETE FROM ODIN_PRD.RPT.CONTENT_PARTNER_WEEKLY WHERE EVENT_DATE_EASTERN >= $WEEKLY_START;`});
        etlLoad2.execute();
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`INSERT INTO ODIN_PRD.RPT.CONTENT_PARTNER_WEEKLY
                            (event_date_eastern, CLIENT_ID, session_id, user_country_id, 
                            episode_name, content_partner_id, content_partner_name, 
                            parent_channel_name, parent_partner_name, series_name, clip_name, 
                            tableau_emails, total_viewing_minutes)
                            SELECT DATE_TRUNC('week', event_date_eastern), CLIENT_ID, session_id, user_country_id, 
                            episode_name, content_partner_id, content_partner_name, 
                            parent_channel_name, parent_partner_name, series_name, clip_name, 
                            tableau_emails, SUM(total_viewing_minutes) total_viewing_minutes
                            FROM ODIN_PRD.RPT.CONTENT_PARTNER_DAILY
                            WHERE date_trunc('week',event_date_eastern) = $WEEKLY_START
                            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;`});
        res = etlLoad3.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    $$;

CREATE OR REPLACE PROCEDURE RPT.LOAD_CONTENT_PARTNER_MONTHLY (DAG_ID varchar(50), TASK_ID varchar(50), EXECUTION_DATE varchar(50))
    RETURNS VARCHAR(250)
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION SET TIMEZONE = 'America/New_York'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to load data into the RPT.CONTENT_PARTNER_MONTHLY table' FROM DUAL"}); 
                                     
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad1 = snowflake.createStatement(
          {sqlText:`SET MONTHLY_START = (SELECT DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())));`});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`DELETE FROM ODIN_PRD.RPT.CONTENT_PARTNER_MONTHLY WHERE EVENT_DATE_EASTERN >= $MONTHLY_START;`});
        etlLoad2.execute();
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`INSERT INTO ODIN_PRD.RPT.CONTENT_PARTNER_MONTHLY
                        (event_date_eastern, CLIENT_ID, session_id, user_country_id, 
                        episode_name, content_partner_id, content_partner_name, 
                        parent_channel_name, parent_partner_name, series_name, clip_name, 
                        tableau_emails, total_viewing_minutes)
                        SELECT DATE_TRUNC('month', event_date_eastern), CLIENT_ID, session_id, user_country_id, 
                        episode_name, content_partner_id, content_partner_name, 
                        parent_channel_name, parent_partner_name, series_name, clip_name, 
                        tableau_emails, SUM(total_viewing_minutes) total_viewing_minutes
                        FROM ODIN_PRD.RPT.CONTENT_PARTNER_DAILY
                        WHERE date_trunc('month',event_date_eastern) = $MONTHLY_START
                        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;`});
        res = etlLoad3.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    $$;



    
