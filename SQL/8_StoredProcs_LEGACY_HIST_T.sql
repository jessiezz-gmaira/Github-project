CREATE OR REPLACE PROCEDURE "LOAD_CLIP_MAPPING_LEFT_JOIN_ALL"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create clip_mapping_left_join_all for the incremental load'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
                
        var etlLoad1 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.clip_mapping_left_join_all
                    as
                    select DISTINCT
                    CMS_PARTNER_DIM.PARTNER_ID,
                    CMS_SERIES_DIM.SERIES_ID,
                    CMS_EPISODE_DIM.EPISODE_ID,
                    CMS_CLIP_DIM.CLIP_ID
                    from ODIN_PRD.DW_ODIN.CMS_EPISODE_SOURCES_PARSED_VW 
                    left join ODIN_PRD.DW_ODIN.CMS_CLIP_DIM on CMS_CLIP_DIM.CLIP_ID = CMS_EPISODE_SOURCES_PARSED_VW.clip_id
                    join ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM  on CMS_EPISODE_DIM.EPISODE_id = CMS_EPISODE_SOURCES_PARSED_VW.episode_id 
                    JOIN ODIN_PRD.DW_ODIN.CMS_SERIES_DIM ON CMS_EPISODE_DIM.SERIES_ID = CMS_SERIES_DIM.SERIES_ID
                    left join ODIN_PRD.DW_ODIN.CMS_PARTNER_DIM on CMS_PARTNER_DIM.PARTNER_ID = CMS_CLIP_DIM.PARTNER_ID; `});
        res = etlLoad1.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM LEGACY_HIST_T.clip_mapping_left_join_all) 
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_ETL_EVENTS_COMPOUND_SESSION_IND"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create etl_events_compound_session_ind that only contains data to be updated'' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);


        var etlLoad1 = snowflake.createStatement(
          {sqlText:`----etl_events_compound_session_ind
                    --Calculates the number of seconds until the next event, per client id, and whether the event is the start of a session or not
                    --Can be dropped and recreated
                    CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.etl_events_compound_session_ind AS 
                    SELECT PLUTO_EVENTS_DEDUPLICATED.*,
                    --number of seconds until the next event
                    --if it''s over 30 minutes, then the next event will be the start of a new session, and the current session length is 30 minutes
                    --the last event of the session is going to have ''1800'' for the value
                    LEAST(1800,
                            --the difference between the current timestamp and the next timestamp, for that client id
                            --if it''s the last event for that client, then default to 30 minutes
                            --the difference is calculated in ms and then rounded up to s, so that the difference in s is rounded accurately. example: ''2019-12-01 19:45:31.220'' to ''2019-12-01 19:45:50.977''        
                              COALESCE(ROUND(DATEDIFF(''millisecond'', request_datetime, LEAD(request_datetime) OVER (PARTITION BY anon_user_id ORDER BY request_datetime asc, event_id asc)) / 1000)
                                , 1800),
                            --if the session goes over to the next day, the session length will be from the start time to midnight
                            --next day counts as a new session
                              ROUND(DATEDIFF(''millisecond'', request_datetime, CAST(DATEADD(''day'', 1, request_date) AS TIMESTAMP)) / 1000)) AS TIME_TO_NEXT_EVENT_S,

                    --whether the session is the first session for that watch period. 3 conditions
                    --Condition 1: for that particular client id, it''s the first event
                    --Condition 2: the difference between the previous datetime and the current datetime, for that anon user_id, is over 1 second
                    --Condition 3: for a anon user id, the previous request date is different than the current request date (going into a new day)
                        CASE WHEN LAG(anon_user_id) OVER (PARTITION BY anon_user_id ORDER BY request_datetime, event_id) IS NULL THEN TRUE
                            WHEN ROUND(DATEDIFF(''millisecond'', LAG(request_datetime) OVER (PARTITION BY anon_user_id ORDER BY request_datetime, event_id), request_datetime) / 1000.0) >= 1800 THEN TRUE
                            WHEN request_date <> LAG(request_date) OVER (PARTITION BY anon_user_id ORDER BY request_datetime, event_id) THEN TRUE
                            ELSE FALSE
                        END AS is_session_start
                        from LEGACY_HIST_T.PLUTO_EVENTS_DEDUPLICATED`});
        etlLoad1.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM  LEGACY_HIST_T.etl_events_compound_session_ind)
                      , STATUS = ''Succeeded.'' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_ETL_EVENTS_SESSIONIZED"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create ETL_EVENTS_SESSIONIZED from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.etl_events_sessionized
                      AS
                      SELECT
                          etl_events_compound_session_ind.*,
                          ETL_SESSIONS_BOUNDARIES.session_id
                        FROM LEGACY_HIST_T.etl_events_compound_session_ind
                        LEFT JOIN LEGACY_HIST_T.ETL_SESSIONS_BOUNDARIES
                        -- event_id CANNOT be used as a substitute join key here
                          ON etl_events_compound_session_ind.anon_user_id = ETL_SESSIONS_BOUNDARIES.anon_user_id
                          -- non-equi-join will be slow
                          AND etl_events_compound_session_ind.request_datetime >= ETL_SESSIONS_BOUNDARIES.session_lower_bound
                          AND etl_events_compound_session_ind.request_datetime < ETL_SESSIONS_BOUNDARIES.session_upper_bound`});
        res = etlLoad2.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM LEGACY_HIST_T.etl_events_sessionized) 
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_ETL_SESSIONS_BOUNDARIES"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create ETL_SESSIONS_BOUNDARIES from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.ETL_SESSIONS_BOUNDARIES
                      AS
                      SELECT
                          SESSION_START.event_id,
                          SESSION_START.anon_user_id,
                          CAST(ROW_NUMBER() OVER (PARTITION BY SESSION_START.anon_user_id ORDER BY SESSION_START.request_datetime, SESSION_START.event_id) AS VARCHAR) 
                             || ''-'' || SESSION_START.anon_user_id AS session_id,
                          SESSION_START.request_datetime AS session_lower_bound,
                          LEAST(
                            COALESCE(
                              LEAD(SESSION_START.request_datetime) OVER (PARTITION BY SESSION_START.anon_user_id ORDER BY SESSION_START.request_datetime, SESSION_START.event_id)
                              , CAST(DATEADD(''day'', 1, SESSION_START.request_date) AS TIMESTAMP)),
                            CAST(DATEADD(''day'', 1, SESSION_START.request_date) AS TIMESTAMP)) AS session_upper_bound
                        FROM LEGACY_HIST_T.SESSION_START;`});
        res = etlLoad2.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM LEGACY_HIST_T.ETL_SESSIONS_BOUNDARIES) 
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_PLUTO_ACTIVE_USER_SESSIONS"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create pluto_active_user_sessions from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var startDate = snowflake.createStatement(
            {sqlText: "SET START_DATE = (SELECT CDC_UTC FROM LEGACY_HIST_T.T_ETL_LAST_UPDATE WHERE TASK = ''T_START_STOP_TIMES'');"});
        startDate.execute();
        
        var endDate = snowflake.createStatement(
            {sqlText: "SET END_DATE = (SELECT DATEADD(''day'', -1, CURRENT_DATE()));"});
        endDate.execute();
        
        var etlLoad1 = snowflake.createStatement(
            {sqlText: "DELETE FROM LEGACY_HIST_T.pluto_active_user_sessions WHERE REQUEST_DATE BETWEEN $START_DATE AND $END_DATE"});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`INSERT INTO LEGACY_HIST_T.pluto_active_user_sessions 
                    SELECT
                      etl_events_sessionized.anon_user_id,
                      etl_events_sessionized.session_id,
                      etl_events_sessionized.request_date,
                      top_platforms.platform,
                      top_country_codes.country_code,
                      MIN(etl_events_sessionized.request_datetime) AS session_start,
                      LEAST(
                          MAX(etl_events_sessionized.request_datetime),
                          CAST(DATEADD(''day'', 1, etl_events_sessionized.request_date) AS TIMESTAMP)
                      )
                      AS session_end,
                      COUNT(etl_events_sessionized.event_id) AS event_cnt,
                      COUNT(DISTINCT etl_events_sessionized.clip_id) AS clip_cnt
                    FROM LEGACY_HIST_T.etl_events_sessionized 
                    JOIN LEGACY_HIST_T.top_platforms
                      ON etl_events_sessionized.anon_user_id = top_platforms.anon_user_id 
                      AND etl_events_sessionized.session_id = top_platforms.session_id
                    JOIN LEGACY_HIST_T.top_country_codes
                      ON etl_events_sessionized.anon_user_id = top_country_codes.anon_user_id 
                      AND etl_events_sessionized.session_id = top_country_codes.session_id
                    WHERE etl_events_sessionized.REQUEST_DATE BETWEEN $START_DATE AND $END_DATE
                    GROUP BY 1, 2, 3, 4,5
                    HAVING COUNT(etl_events_sessionized.event_id) > 5
                      AND DATEDIFF(
                        ''second'',
                        MIN(etl_events_sessionized.request_datetime),
                        LEAST
                        (
                          MAX(etl_events_sessionized.request_datetime),
                          CAST(DATEADD(''day'', 1, etl_events_sessionized.request_date) AS TIMESTAMP)
                        )
                      ) >= 15 `});
        res = etlLoad2.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted + `
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_PLUTO_CLIP_START_STOP_TIMES"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create pluto_clip_start_stop_times from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var startDate = snowflake.createStatement(
            {sqlText: "SET START_DATE = (SELECT CDC_UTC FROM LEGACY_HIST_T.T_ETL_LAST_UPDATE WHERE TASK = ''T_START_STOP_TIMES'');"});
        startDate.execute();
        
        var endDate = snowflake.createStatement(
            {sqlText: "SET END_DATE = (SELECT DATEADD(''day'', -1, CURRENT_DATE()));"});
        endDate.execute();
        
        var etlLoad1 = snowflake.createStatement(
            {sqlText: "DELETE FROM RPT.pluto_clip_start_stop_times WHERE REQUEST_DATE BETWEEN $START_DATE AND $END_DATE;"});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`insert into RPT.pluto_clip_start_stop_times
                    (EVENT_ID, IP, COUNTRY_CODE, COUNTRY, REGION_NAME, REGION, CITY, POSTAL_CODE, DMA_CODE, OS, BROWSER, ARCHITECTURE, PLATFORM,
                    REQUEST_DATE, REQUEST_DATETIME, URL, REFERRER, UA, SOURCE, ACTION, CATEGORY, LABEL, VALUE, CHANNEL_ID, EPISODE_ID, CLIP_ID, ANON_USER_ID, USER_ID,
                    SESSION_ID, CLIP_START, CLIP_END, CLIP_DURATION, SESSION_START, SESSION_END, EVENT_CNT)
                     SELECT EVENT_ID, IP, COUNTRY_CODE, COUNTRY, REGION_NAME, REGION, CITY, POSTAL_CODE, DMA_CODE, OS, BROWSER, ARCHITECTURE, PLATFORM,
                    REQUEST_DATE, REQUEST_DATETIME, URL, REFERRER, UA, SOURCE, ACTION, CATEGORY, LABEL, VALUE, CHANNEL_ID, EPISODE_ID, CLIP_ID, ANON_USER_ID, USER_ID,
                    SESSION_ID, CLIP_START, CLIP_END, CLIP_DURATION, SESSION_START, SESSION_END, EVENT_CNT
                    FROM (
                      SELECT
                        etl_events_sessionized.*,
                        pluto_active_user_sessions.SESSION_START, pluto_active_user_sessions.SESSION_END, pluto_active_user_sessions.EVENT_CNT,
                        CASE WHEN LAG(etl_events_sessionized.request_datetime) 
                            OVER (PARTITION BY etl_events_sessionized.session_id ORDER BY etl_events_sessionized.request_datetime, etl_events_sessionized.event_id) IS NULL
                          THEN pluto_active_user_sessions.session_start
                          ELSE etl_events_sessionized.request_datetime
                        END AS clip_start,
                        LEAST(COALESCE(
                            LEAD(etl_events_sessionized.request_datetime) 
                            OVER (PARTITION BY etl_events_sessionized.session_id ORDER BY etl_events_sessionized.request_datetime, etl_events_sessionized.event_id)
                            , pluto_active_user_sessions.session_end),
                          pluto_active_user_sessions.session_end) AS clip_end,
                        CAST(ROUND(DATEDIFF(''millisecond'', clip_start, clip_end) / 1000.0) AS INT) AS clip_duration
                      FROM LEGACY_HIST_T.etl_events_sessionized
                      JOIN LEGACY_HIST_T.pluto_active_user_sessions
                        ON etl_events_sessionized.anon_user_id = pluto_active_user_sessions.anon_user_id
                        AND etl_events_sessionized.session_id = pluto_active_user_sessions.session_id
                        AND etl_events_sessionized.REQUEST_DATE = pluto_active_user_sessions.REQUEST_DATE
                      WHERE etl_events_sessionized.action = ''clip''
                        AND etl_events_sessionized.category = ''watch''
                        AND etl_events_sessionized.REQUEST_DATE BETWEEN $START_DATE AND $END_DATE
                    )
                        where clip_start < clip_end; `});
        res = etlLoad2.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.pluto_clip_start_stop_times
                    SET APP_NAME = 
                    (CASE 
                    WHEN url ILIKE ''%event_source=android%''  and  ua ILIKE ''%KF%'' THEN ''firetablet'' --mobile
                    WHEN ua ilike ''%AFT%''  THEN ''firetv'' --CTV
                    WHEN ua ilike ''%PortalTV%''  THEN ''facebook''
                    WHEN url ILIKE ''%event_source=catalyst%'' THEN ''catalyst''--CTV
                    when ua ILIKE ''%hisense%'' then ''hisense'' --CTV
                    WHEN ua ILIKE ''%bravia%'' then ''sonytv'' --CTV
                    WHEN ua ILIKE ''% MI %'' then ''androidtv''--CTV
                    WHEN ua ILIKE ''%MIBOX%'' then ''androidtv''--CTV
                    WHEN ua ILIKE ''%XIAOMI%'' then ''androidtv''--CTV
                    WHEN url ILIKE ''%event_source=ps4%'' then ''playstation 4'' --CTV
                    WHEN url ILIKE ''%event_source=sony-ps3%'' then ''playstation 3'' --CTV
                    WHEN url ILIKE ''%event_source=ps3%'' then ''playstation 3'' --CTV
                    WHEN url ILIKE ''%event_source=android_oculus%'' then ''oculus'' --mobile
                    when url ILIKE ''%event_source=samsung-tizen%'' THEN ''samsungtizen'' --CTV
                    when url ILIKE ''%event_source=samsung-orsay%'' THEN ''samsungorsay'' --CTV
                    WHEN (url ILIKE ''%event_source=roku%'' or platform ILIKE ''%roku%'') THEN (CASE WHEN lower(country_code) = ''gb'' then ''rokuuk'' else ''roku'' end)  --CTV
                    When url ILIKE ''%event_source=tvos%'' then ''tvos''  --CTV
                    When url ILIKE ''%event_source=ios%'' then ''ios''  --mobile
                    when url ILIKE ''%event_source=vizio-mtk%'' THEN ''viziovia2.x'' --CTV
                    WHEN url ILIKE ''%event_source=vizio-sigma%'' THEN ''viziovia2.x'' --CTV
                    WHEN url ILIKE ''%event_source=vizio-other%'' THEN ''viziovia2.x'' --CTV
                    WHEN url ILIKE ''%event_source=viziovia%'' THEN ''viziovia3.x''--CTV
                    WHEN url ILIKE ''%event_source=vizio-via%'' THEN ''viziovia3.x''--CTV
                    WHEN url ILIKE ''%event_source=vizio_receiver%'' THEN ''viziosmartcast''--CTV
                    WHEN url ILIKE ''%event_source=chromecast%'' then ''chromecast'' --CTV
                    WHEN url ILIKE ''%event_source=skyticket%'' then ''skyticket'' --CTV
                    WHEN lower(url) IN (''%event_source=chrome%'' , ''%event_source=asus%'')  then ''chromeos'' --mobile
                    WHEN url ILIKE ''%event_source=xbox360%'' then ''xbox'' --CTV
                    WHEN (url ILIKE ''%event_source=now-tv%'' or url ILIKE ''%event_source=nowtv%'') then ''nowtv'' --CTV
                    WHEN lower(platform) = ''desktop'' THEN ''desktop''  
                    WHEN lower(platform) = ''embed'' THEN ''weblegacy''  
                    WHEN (lower(platform) <> ''desktop'' and url ilike ''%event_source=web%'') THEN ''weblegacy''
                    WHEN url ilike ''%event_source=android%'' THEN (CASE WHEN ua ilike ''%CTV%'' THEN ''androidtv'' ELSE ''androidmobile'' END) --mobile --CTV
                    else ''NA''
                    end),
                    APP_VERSION = regexp_substr(pluto_clip_start_stop_times.url, ''[?|&]app_version=([^&]+)'',1,1, ''e'')
                    WHERE  REQUEST_DATE BETWEEN $START_DATE AND $END_DATE
                        AND (pluto_clip_start_stop_times.APP_NAME IS NULL
        OR pluto_clip_start_stop_times.APP_VERSION IS NULL);`});
        etlLoad3.execute();
        
        var etlLoad4 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.pluto_clip_start_stop_times
                    SET LIVE_FLAG = 
                    (CASE WHEN APP_NAME = ''firetv'' AND URL ILIKE ''%event_source=android_fire_livetv%'' THEN ''Y'' ELSE ''N'' end)
                    WHERE LIVE_FLAG IS NULL;`});
        etlLoad4.execute();
        
        var etlLoad5 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.pluto_clip_start_stop_times
                    SET ANON_USER_ID = SUBSTRING(ANON_USER_ID, 6)
                    WHERE SUBSTRING(ANON_USER_ID, 0, 5) = ''ROKU-'';`});
        etlLoad5.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted + `
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_PLUTO_EVENTS_DEDUPLICATED"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create PLUTO_EVENTS_DEDUPLICATED that only contains data to be updated'' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);


        var etlLoad1 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.PLUTO_EVENTS_DEDUPLICATED
                        AS
                        SELECT * FROM (
                        SELECT PLUTO_EVENTS.*
                        , ROW_NUMBER() OVER (PARTITION BY PLUTO_EVENTS.EVENT_ID ORDER BY PLUTO_EVENTS.REQUEST_DATETIME ASC) ROWNUM
                        FROM LEGACY_HIST_T.PLUTO_EVENTS_INCREMENTAL_LOAD PLUTO_EVENTS
                        WHERE (platform <> ''Embed'' OR action NOT IN (''playAd'', ''adError'')))
                        WHERE ROWNUM = 1`});
        etlLoad1.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM  LEGACY_HIST_T.PLUTO_EVENTS_DEDUPLICATED)
                      , STATUS = ''Succeeded.'' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_PLUTO_EVENTS_INCREMENTAL_LOAD"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create PLUTO_EVENTS_INCREMENTAL_LOAD that only contains data to be updated'' FROM DUAL"}); 
    
    var updateLastUpdate = snowflake.createStatement(
      {sqlText: `UPDATE LEGACY_HIST_T.T_ETL_LAST_UPDATE SET CDC_UTC = (SELECT MAX(REQUEST_DATE) FROM RPT.pluto_clip_start_stop_times) WHERE TASK = ''T_START_STOP_TIMES'';`});                                   
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        updateLastUpdate.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var setStartDate = snowflake.createStatement(
            {sqlText: "SET START_DATE = (SELECT CDC_UTC FROM LEGACY_HIST_T.T_ETL_LAST_UPDATE WHERE TASK = ''T_START_STOP_TIMES'')"});
        setStartDate.execute();
        
        var setEndDate = snowflake.createStatement(
            {sqlText: "SET END_DATE = (SELECT DATEADD(''day'', -1, CURRENT_DATE()))"});
        setEndDate.execute();

        var etlLoad1 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.PLUTO_EVENTS_INCREMENTAL_LOAD
                            AS SELECT * FROM LEGACY_HIST_T.PLUTO_EVENTS 
                            WHERE REQUEST_DATE BETWEEN $START_DATE AND $END_DATE`});
        etlLoad1.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM  LEGACY_HIST_T.PLUTO_EVENTS_INCREMENTAL_LOAD)
                      , STATUS = ''Succeeded.'' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';


CREATE OR REPLACE PROCEDURE "LOAD_PLUTO_KPIS_HOURLY_LEFT_JOINS_STAGE"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$

    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to create pluto_kpis_hourly_left_joins_stage from incremental data' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        var etlLoad1 = snowflake.createStatement(
            {sqlText: "UPDATE LEGACY_HIST_T.T_ETL_LAST_UPDATE SET CDC_UTC = (SELECT MAX(CLIP_START) FROM LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage)  WHERE TASK = 'T_PLUTO_KPIS_STAGE';"});
        etlLoad1.execute();
        
        var startDate = snowflake.createStatement(
            {sqlText: "SET START_DATE = (SELECT DATEADD('day', -1, (SELECT CDC_UTC FROM LEGACY_HIST_T.T_ETL_LAST_UPDATE WHERE TASK = 'T_PLUTO_KPIS_STAGE')));"});
        startDate.execute();
        
        var endDate = snowflake.createStatement(
            {sqlText: "SET END_DATE = (SELECT DATEADD('day', -1, CURRENT_DATE()));"});
        endDate.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE LEGACY_HIST_T.pluto_clip_start_stop_times_TEMP
                AS SELECT * FROM RPT.pluto_clip_start_stop_times SS
                WHERE  SS.REQUEST_DATE BETWEEN $START_DATE AND $END_DATE;`});
        etlLoad2.execute();
        var etlLoad3 = snowflake.createStatement(
            {sqlText: `CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage
                        AS 
                          WITH VOD_COLLECTION AS (SELECT EPISODE_ID, PARSED_COUNTRY_CODE FROM ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW
                            UNION 
                            SELECT CMS_EPISODE_DIM.EPISODE_ID, PARSED_COUNTRY_CODE                          
                            FROM ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM
                            JOIN  ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW ON CMS_EPISODE_DIM.SERIES_ID = CMS_VODCATEGORIES_PARSED_REGION_VW.SERIES_ID)
                            
                        SELECT 
                            SS.CLIP_START,
                            SS.CLIP_END,
                            SS.ANON_USER_ID AS CLIENT_ID,
                            SS.APP_NAME,
                            SS.APP_VERSION,
                            SS.LIVE_FLAG,
                            SS.SESSION_ID,
                            'vod' AS channel_id,
                            clip_mapping_left_join_all.SERIES_ID,
                            SS.EPISODE_ID,
                            SS.CLIP_ID,
                            clip_mapping_left_join_all.PARTNER_ID,
                            SS.IP AS CLIENT_IP,
                            HIST.RPT.f_inet_aton(SS.IP) as CLIENT_IP_INTEGER,
                            SS.COUNTRY_CODE AS COUNTRY,
                            SS.CITY AS CITY,
                            SS.REGION_NAME AS REGION,
                            SS.DMA_CODE AS DMA_CODE,
                            NULL AS GEO_ALIGNED_FLAG,
                            CASE WHEN (VOD_COLLECTION.EPISODE_ID IS NOT NULL
                                AND SS.CLIP_START BETWEEN CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_START_UTC AND CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_END_UTC)
                                AND UPPER(CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.DISTRIBUTION_TYPE) = 'AVOD'
                                THEN TRUE
                                ELSE FALSE END AS TIMELINE_ALIGNED_FLAG,
                            CASE WHEN (ss.episode_id = clip_mapping_left_join_all.episode_id AND ss.clip_id = clip_mapping_left_join_all.clip_id) 
                                THEN TRUE
                                ELSE FALSE END AS EP_SOURCES_ALIGNED_FLAG,
                            CASE WHEN (ss.clip_id = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.clip_id 
                                AND ss.CLIP_START BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC)
                                AND UPPER(CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.distribution_type) = 'AVOD'
                                THEN TRUE
                                ELSE FALSE END AS CLIP_WINDOW_ALIGNED_FLAG,
                            SUM(ss.clip_duration) AS TOTAL_VIEWING_SECONDS
                        FROM LEGACY_HIST_T.pluto_clip_start_stop_times_TEMP ss
                        LEFT JOIN VOD_COLLECTION 
                            ON SS.EPISODE_ID = VOD_COLLECTION.EPISODE_ID
                            AND UPPER(SS.COUNTRY_CODE) = UPPER(VOD_COLLECTION.PARSED_COUNTRY_CODE)
                        LEFT JOIN ODIN_PRD.DW_ODIN.CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW
                            on ss.EPISODE_ID = CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_ID
                            AND SS.CLIP_START BETWEEN CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_START_UTC AND CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_END_UTC
                            AND UPPER(CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.DISTRIBUTION_TYPE) = 'AVOD'
                            
                        LEFT JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_AVAIL_WINDOWS_PARSED_VW
                            ON ss.clip_id = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.clip_id
                            AND ss.CLIP_START BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC
                            AND UPPER(CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.distribution_type) = 'AVOD'
                        LEFT JOIN LEGACY_HIST_T.clip_mapping_left_join_all 
                            ON ss.EPISODE_ID = clip_mapping_left_join_all.EPISODE_ID 
                            AND SS.CLIP_ID = clip_mapping_left_join_all.CLIP_ID
                        WHERE SS.CHANNEL_ID ilike '%vod%'
                        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
                        UNION ALL
                        SELECT 
                            SS.CLIP_START,
                            SS.CLIP_END,
                            SS.ANON_USER_ID AS CLIENT_ID,
                            SS.APP_NAME,
                            SS.APP_VERSION,
                            SS.LIVE_FLAG,
                            SS.SESSION_ID,
                            SS.channel_id,
                            clip_mapping_left_join_all.SERIES_ID,
                            SS.EPISODE_ID,
                            SS.CLIP_ID,
                            clip_mapping_left_join_all.PARTNER_ID,
                            SS.IP AS CLIENT_IP,
                            HIST.RPT.f_inet_aton(SS.IP) as CLIENT_IP_INTEGER,
                            SS.COUNTRY_CODE AS COUNTRY,
                            SS.CITY AS CITY,
                            SS.REGION_NAME AS REGION,
                            SS.DMA_CODE AS DMA_CODE,
                            CASE WHEN (CMS_CHANNEL_DIM.PARSED_COUNTRY_CODE IS NOT NULL
                                AND UPPER(SS.COUNTRY_CODE) = UPPER(CMS_CHANNEL_DIM.PARSED_COUNTRY_CODE)) THEN TRUE
                                ELSE FALSE END AS GEO_ALIGNED_FLAG,
                            CASE WHEN (ss.channel_id = CMS_MONGO_TIMELINES_OVERWRITE.channel_id
                                AND ss.episode_id = CMS_MONGO_TIMELINES_OVERWRITE.episode_id
                                AND ss.CLIP_START BETWEEN CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_START_UTC AND CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_STOP_UTC)
                                THEN TRUE
                                ELSE FALSE END AS TIMELINE_ALIGNED_FLAG,
                            CASE WHEN (ss.episode_id = clip_mapping_left_join_all.episode_id AND ss.clip_id = clip_mapping_left_join_all.clip_id) 
                                THEN TRUE
                                ELSE FALSE END AS EP_SOURCES_ALIGNED_FLAG,
                            CASE WHEN (ss.clip_id = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.clip_id
                                AND ss.CLIP_START BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC
                                --AND distribute_as_linear = TRUE
                                )
                                THEN TRUE
                                ELSE FALSE END AS CLIP_WINDOW_ALIGNED_FLAG,
                            SUM(ss.clip_duration) AS TOTAL_VIEWING_SECONDS
                        FROM LEGACY_HIST_T.pluto_clip_start_stop_times_TEMP ss
                        LEFT JOIN ODIN_PRD.STG.CMS_MONGO_TIMELINES_OVERWRITE
                            ON ss.channel_id = CMS_MONGO_TIMELINES_OVERWRITE.channel_id
                            AND ss.episode_id = CMS_MONGO_TIMELINES_OVERWRITE.episode_id
                            AND ss.CLIP_START BETWEEN CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_START_UTC AND CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_STOP_UTC
                        LEFT JOIN ODIN_PRD.DW_ODIN.CMS_CLIP_AVAIL_WINDOWS_PARSED_VW
                            ON ss.clip_id = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.clip_id
                            AND ss.CLIP_START BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC
                            AND UPPER(CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.distribution_type) = 'LINEAR'
                        LEFT JOIN LEGACY_HIST_T.clip_mapping_left_join_all 
                            ON ss.EPISODE_ID = clip_mapping_left_join_all.EPISODE_ID 
                            AND SS.CLIP_ID = clip_mapping_left_join_all.CLIP_ID
                        LEFT JOIN ODIN_PRD.DW_ODIN.CMS_CHANNEL_REGIONS_PARSED_VW CMS_CHANNEL_DIM 
                            ON SS.CHANNEL_ID = CMS_CHANNEL_DIM.CHANNEL_ID
                            AND UPPER(SS.COUNTRY_CODE) = UPPER(CMS_CHANNEL_DIM.PARSED_COUNTRY_CODE)
                        WHERE SS.CHANNEL_ID not ilike '%vod%'
                        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;`});
        etlLoad3.execute();
        var etlLoad4 = snowflake.createStatement(
            {sqlText: `CREATE OR REPLACE TEMPORARY TABLE T_VOD_GEO_ALIGNED AS 
                        (SELECT pluto_kpis_hourly_left_joins_stage.*
                        FROM HIST.LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage
                         WHERE pluto_kpis_hourly_left_joins_stage.CHANNEL_ID ilike '%vod%' 
                         AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)
                         AND EXISTS (SELECT 1
                                       FROM ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW
                                       WHERE CMS_VODCATEGORIES_PARSED_REGION_VW.EPISODE_ID = pluto_kpis_hourly_left_joins_stage.EPISODE_ID
                                       AND UPPER(pluto_kpis_hourly_left_joins_stage.COUNTRY) = UPPER(CMS_VODCATEGORIES_PARSED_REGION_VW.PARSED_COUNTRY_CODE))
                          UNION
                         
                         SELECT pluto_kpis_hourly_left_joins_stage.*
                         FROM HIST.LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage
                         WHERE pluto_kpis_hourly_left_joins_stage.CHANNEL_ID ilike '%vod%' 
                         AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)
                         AND EXISTS (SELECT 1
                                       FROM ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW
                                       WHERE CMS_VODCATEGORIES_PARSED_REGION_VW.SERIES_ID = pluto_kpis_hourly_left_joins_stage.SERIES_ID
                                       AND UPPER(pluto_kpis_hourly_left_joins_stage.COUNTRY) = UPPER(CMS_VODCATEGORIES_PARSED_REGION_VW.PARSED_COUNTRY_CODE))
                        );`});
        etlLoad4.execute();
        var etlLoad5 = snowflake.createStatement(
          {sqlText:`UPDATE HIST.LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage
                      SET GEO_ALIGNED_FLAG = TRUE
                      FROM T_VOD_GEO_ALIGNED GEO_ALIGNED
                      WHERE pluto_kpis_hourly_left_joins_stage.CHANNEL_ID ilike '%vod%' 
                      AND pluto_kpis_hourly_left_joins_stage.CLIP_START = GEO_ALIGNED.CLIP_START
                      AND pluto_kpis_hourly_left_joins_stage.CLIP_END = GEO_ALIGNED.CLIP_END
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.CLIENT_ID, 'NA') = IFNULL(GEO_ALIGNED.CLIENT_ID, 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.APP_NAME, 'NA') = IFNULL(GEO_ALIGNED.APP_NAME , 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.APP_VERSION, 'NA') = IFNULL(GEO_ALIGNED.APP_VERSION , 'NA')
                      AND pluto_kpis_hourly_left_joins_stage.LIVE_FLAG = GEO_ALIGNED.LIVE_FLAG
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.SESSION_ID, 'NA') = IFNULL(GEO_ALIGNED.SESSION_ID , 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.SERIES_ID, 'NA') = IFNULL(GEO_ALIGNED.SERIES_ID , 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.EPISODE_ID , 'NA')= IFNULL(GEO_ALIGNED.EPISODE_ID, 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.CLIP_ID, 'NA') = IFNULL(GEO_ALIGNED.CLIP_ID , 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.PARTNER_ID, 'NA') = IFNULL(GEO_ALIGNED.PARTNER_ID , 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.CLIENT_IP, 'NA') = IFNULL(GEO_ALIGNED.CLIENT_IP, 'NA') 
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.COUNTRY, 'NA') = IFNULL(GEO_ALIGNED.COUNTRY, 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.CITY, 'NA') = IFNULL(GEO_ALIGNED.CITY , 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.REGION, 'NA') = IFNULL(GEO_ALIGNED.REGION , 'NA')
                      AND pluto_kpis_hourly_left_joins_stage.DMA_CODE = IFNULL(GEO_ALIGNED.DMA_CODE , 'NA')
                      AND IFNULL(pluto_kpis_hourly_left_joins_stage.COUNTRY, 'NA') = IFNULL(GEO_ALIGNED.COUNTRY, 'NA');`});
        etlLoad5.execute();
        var etlLoad6 = snowflake.createStatement(
            {sqlText: `UPDATE HIST.LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage
                          SET GEO_ALIGNED_FLAG = FALSE
                          WHERE GEO_ALIGNED_FLAG IS NULL AND CHANNEL_ID ilike '%vod%' ;`});
        etlLoad6.execute();
                        
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage)
                      , STATUS = 'Succeeded.'  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    $$;



CREATE OR REPLACE PROCEDURE "LOAD_SESSION_START"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to add to SESSION_START with incremental data'' FROM DUAL"}); 
    
    var updateLastUpdate = snowflake.createStatement(
      {sqlText: `UPDATE LEGACY_HIST_T.T_ETL_LAST_UPDATE SET CDC_UTC = (SELECT MAX(REQUEST_DATE) FROM RPT.pluto_clip_start_stop_times) 
                    WHERE TASK = ''T_START_STOP_TIMES'';`});                                   
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        updateLastUpdate.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var setStartDate = snowflake.createStatement(
            {sqlText: "SET START_DATE = (SELECT CDC_UTC FROM LEGACY_HIST_T.T_ETL_LAST_UPDATE WHERE TASK = ''T_START_STOP_TIMES'')"});
        setStartDate.execute();
        
        var setEndDate = snowflake.createStatement(
            {sqlText: "SET END_DATE = (SELECT DATEADD(''day'', -1, CURRENT_DATE()))"});
        setEndDate.execute();

        var etlLoad1 = snowflake.createStatement(
          {sqlText:`DELETE FROM LEGACY_HIST_T.SESSION_START WHERE REQUEST_DATE BETWEEN $START_DATE AND $END_DATE;`});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`INSERT INTO LEGACY_HIST_T.SESSION_START
                SELECT * FROM LEGACY_HIST_T.etl_events_compound_session_ind
                WHERE IS_SESSION_START = TRUE 
                AND REQUEST_DATE BETWEEN $START_DATE AND $END_DATE`});
        res = etlLoad2.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_TOP_COUNTRY_CODES"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create TOP_COUNTRY_CODES from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.TOP_COUNTRY_CODES
                    as  SELECT ANON_USER_ID, SESSION_ID, COUNTRY_CODE FROM (SELECT
                        anon_user_id,
                        session_id,
                        country_code,
                        CAST(ROW_NUMBER() OVER (PARTITION BY anon_user_id, session_id ORDER BY COUNT(country_code) DESC) AS INT) AS ranked_frequency
                      FROM LEGACY_HIST_T.etl_events_sessionized
                      GROUP BY 1, 2, 3) ALL_COUNTRY_CODES
                      WHERE ALL_COUNTRY_CODES.RANKED_FREQUENCY = 1`});
        res = etlLoad2.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM LEGACY_HIST_T.TOP_COUNTRY_CODES) 
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';
CREATE OR REPLACE PROCEDURE "LOAD_TOP_PLATFORMS"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create TOP_PLATFORMS from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TRANSIENT TABLE LEGACY_HIST_T.TOP_PLATFORMS
                    AS SELECT ANON_USER_ID, SESSION_ID, PLATFORM FROM (SELECT
                        anon_user_id,
                        session_id,
                        platform,
                        CAST(ROW_NUMBER() OVER (PARTITION BY anon_user_id, session_id ORDER BY COUNT(platform) DESC) AS INT) AS ranked_frequency
                      FROM LEGACY_HIST_T.etl_events_sessionized
                      GROUP BY 1, 2, 3) ALL_PLATFORMS
                      WHERE ALL_PLATFORMS.RANKED_FREQUENCY = 1`});
        res = etlLoad2.execute();
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (SELECT COUNT(*) FROM LEGACY_HIST_T.TOP_PLATFORMS) 
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';

CREATE OR REPLACE PROCEDURE "LOAD_T_HOURLY_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create T_HOURLY_TVS_AGG from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var startDate = snowflake.createStatement(
            {sqlText: "SET START_DATE = (SELECT DATEADD(''day'', -1, (SELECT CDC_UTC FROM LEGACY_HIST_T.T_ETL_LAST_UPDATE WHERE TASK = ''T_PLUTO_KPIS_STAGE'')));"});
        startDate.execute();
        
        var endDate = snowflake.createStatement(
            {sqlText: "SET END_DATE = (SELECT DATEADD(''day'', -1, CURRENT_DATE()));"});
        endDate.execute();

        var etlLoad1 = snowflake.createStatement(
            {sqlText: `DELETE FROM RPT.T_HOURLY_TVS_AGG WHERE DATE_TRUNC(''day'', REQUEST_DATETIME_START_UTC) BETWEEN $START_DATE AND $END_DATE;`});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`INSERT INTO RPT.T_HOURLY_TVS_AGG
                        SELECT
                            HOUR.HOUR_SID,
                            CLIP_START REQUEST_DATETIME_START_UTC,
                            CLIENT_ID,
                            APP_NAME,
                            APP_VERSION,
                            LIVE_FLAG,
                            SESSION_ID,
                            channel_id,
                            SERIES_ID,
                            EPISODE_ID,
                            CLIP_ID,
                            PARTNER_ID,
                            COUNTRY,
                            CITY,
                            REGION,
                            DMA_CODE,
                            GEO_ALIGNED_FLAG,
                            TIMELINE_ALIGNED_FLAG,
                            EP_SOURCES_ALIGNED_FLAG,
                            CLIP_WINDOW_ALIGNED_FLAG,
                            sum(case when DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_START) = HOUR.UTC 
                                                          and DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_END) = HOUR.UTC
                                                          then datediff(''seconds'', T_KPIS_EVENTS.CLIP_START, T_KPIS_EVENTS.CLIP_END)
                                                      when DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_START) < HOUR.UTC  
                                                          and DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_END) = HOUR.UTC
                                                          then datediff(''seconds'', HOUR.UTC, T_KPIS_EVENTS.CLIP_END)
                                                      when DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_START) = HOUR.UTC
                                                          and DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_END) > HOUR.UTC
                                                          then datediff(''seconds'', T_KPIS_EVENTS.CLIP_START, DATEADD(''hour'',1,HOUR.UTC))
                                                      when DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_START) < HOUR.UTC
                                                          and DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_END) > HOUR.UTC
                                                          then 3600 
                                                      end) as TOTAL_VIEWING_SECONDS
                        FROM RPT.T_KPIS_EVENTS   
                        join (select UTC, HOUR_SID from ODIN_PRD.DW_ODIN.HOUR_DIM where UTC between ''2019-01-01'' and current_timestamp()) as HOUR
                          on HOUR.UTC between DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_START) and DATE_TRUNC(''hour'', T_KPIS_EVENTS.CLIP_END)
                        WHERE DATE_TRUNC(''day'', REQUEST_DATETIME_START_UTC) BETWEEN $START_DATE AND $END_DATE
                        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;`});
        res2 = etlLoad2.execute();
        res2.next();
        row_inserted = res2.getColumnValue(1);
                        
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted + `
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';

CREATE OR REPLACE PROCEDURE "LOAD_T_KPIS_EVENTS"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
RETURNS VARCHAR(250)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = ''UTC''"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT ''" + DAG_ID + "'',''" + TASK_ID + "'',''" + EXECUTION_DATE + "'', ''ETL to create T_KPIS_EVENTS from incremental data'' FROM DUAL"}); 
                                   
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from LEGACY_HIST_T.T_ETL_BATCH_AUDIT WHERE TASK_ID = ''" + TASK_ID + "'' AND DAG_ID = ''" + DAG_ID + "'' AND EXECUTION_DATE = ''" + EXECUTION_DATE + "''"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var startDate = snowflake.createStatement(
            {sqlText: "SET START_DATE = (SELECT DATEADD(''day'', -1, (SELECT CDC_UTC FROM LEGACY_HIST_T.T_ETL_LAST_UPDATE WHERE TASK = ''T_PLUTO_KPIS_STAGE'')));"});
        startDate.execute();
        
        var endDate = snowflake.createStatement(
            {sqlText: "SET END_DATE = (SELECT DATEADD(''day'', -1, CURRENT_DATE()));"});
        endDate.execute();

        var etlLoad1 = snowflake.createStatement(
            {sqlText: `DELETE FROM RPT.T_KPIS_EVENTS WHERE DATE_TRUNC(''day'', CLIP_START) BETWEEN $START_DATE AND $END_DATE;`});
        etlLoad1.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`INSERT INTO RPT.T_KPIS_EVENTS  
                      (
                      select
                        CLIP_START ,
                        CLIP_END,
                        CLIENT_ID,
                        APP_NAME,
                        APP_VERSION,
                        LIVE_FLAG,
                        SESSION_ID,
                        channel_id,
                        SERIES_ID,
                        EPISODE_ID,
                        CLIP_ID,
                        PARTNER_ID,
                        COUNTRY,
                        CITY,
                        REGION,
                        DMA_CODE,
                        GEO_ALIGNED_FLAG,
                        TIMELINE_ALIGNED_FLAG,
                        EP_SOURCES_ALIGNED_FLAG,
                        CLIP_WINDOW_ALIGNED_FLAG,
                        mm_geo.geoname_id,
                      sum(total_viewing_seconds) as total_viewing_seconds
                      from LEGACY_HIST_T.pluto_kpis_hourly_left_joins_stage kpi  
                      LEFT JOIN ODIN_PRD.STG.MAXMIND_COMBINED AS mm_geo ON REGEXP_SUBSTR(kpi.client_ip, ''\\\\d+\\.\\\\d+'') = mm_geo.first_16_bits 
                        AND kpi.client_ip_integer BETWEEN mm_geo.network_start_integer AND mm_geo.network_last_integer  
                      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
                      );`});
        res2 = etlLoad2.execute();
        res2.next();
        row_inserted = res2.getColumnValue(1);
                        
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted + `
                      , STATUS = ''Succeeded.''  
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1)) `});
        updateEtlBatchAuditSuccess.execute();
          
        return "Succeeded. ";}
        
      catch (err)  {
        var updateEtlBatchAuditFail = snowflake.createStatement(
          {sqlText: `MERGE INTO LEGACY_HIST_T.T_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) LEGACY_HIST_T_ETL_BATCH_AUDIT
                      ON LEGACY_HIST_T_ETL_BATCH_AUDIT.BATCH_ID = T_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      STATUS = (?)
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))
                      `, binds: [err.message]
                      });
        updateEtlBatchAuditFail.execute();
 
        return "Failed: " + err.message;}
    
    ';