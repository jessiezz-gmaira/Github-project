!set variable_substitution=true;

USE DATABASE &{snowflakeDatabase};
USE ROLE &{snowflakeRole};
USE SCHEMA STG;
USE WAREHOUSE &{snowflakeWarehouse};

USE SCHEMA RPT;

CREATE OR REPLACE PROCEDURE "LOAD_ACTIVE_SESSION"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
     var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to merge into ACTIVE_SESSION for KPI calculations' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.ACTIVE_SESSION
                      USING
                      (
                          select CLIENT_VIDEO_SEGMENTS.CLIENT_SID
                          ,CLIENT_VIDEO_SEGMENTS.SESSION_ID
                          ,CLIENT_VIDEO_SEGMENTS.APP_SID
                          ,sum(datediff('seconds', CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_BEGIN_UTC, CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC)) as TOTAL_VIEWING_SECONDS

                          from DW_ODIN.CLIENT_VIDEO_SEGMENTS CLIENT_VIDEO_SEGMENTS

                          JOIN DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED ON CLIENT_VIDEO_SEGMENTS.SESSION_ID = CLIENT_SESSION_TO_BE_UPDATED.SESSION_ID
                              AND CLIENT_VIDEO_SEGMENTS.CLIENT_SID = CLIENT_SESSION_TO_BE_UPDATED.CLIENT_SID

                          group by CLIENT_VIDEO_SEGMENTS.CLIENT_SID
                          ,CLIENT_VIDEO_SEGMENTS.SESSION_ID
                          ,CLIENT_VIDEO_SEGMENTS.APP_SID
                          having TOTAL_VIEWING_SECONDS > 15
                      ) STG_ACTIVE_SESSION
                      ON ACTIVE_SESSION.CLIENT_SID = STG_ACTIVE_SESSION.CLIENT_SID
                      AND IFNULL(ACTIVE_SESSION.SESSION_ID, 'NA') = IFNULL(STG_ACTIVE_SESSION.SESSION_ID, 'NA')
                      AND ACTIVE_SESSION.APP_SID = STG_ACTIVE_SESSION.APP_SID
                      WHEN MATCHED AND STG_ACTIVE_SESSION.TOTAL_VIEWING_SECONDS <> ACTIVE_SESSION.TOTAL_VIEWING_SECONDS
                      THEN UPDATE SET ACTIVE_SESSION.TOTAL_VIEWING_SECONDS = STG_ACTIVE_SESSION.TOTAL_VIEWING_SECONDS
                      WHEN NOT MATCHED THEN INSERT (CLIENT_SID, SESSION_ID, APP_SID, TOTAL_VIEWING_SECONDS)
                      VALUES (CLIENT_SID, SESSION_ID, APP_SID, TOTAL_VIEWING_SECONDS)`});
        res = etlLoad.execute();     
        res.next();
        row_inserted = res.getColumnValue(1);
        row_updated = res.getColumnValue(2);                
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, RECORDS_UPDATED = `
                      + row_updated
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
$$
;

CREATE OR REPLACE PROCEDURE "LOAD_CLIENT_VIDEO_EVENT_RAW"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$  
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to insert data into CLIENT_VIDEO_EVENT_RAW that will store the events used to calculate the KPIs' FROM DUAL"}); 
    
    var updateClientVideoEventRawLastUpdate = snowflake.createStatement(
      {sqlText: `UPDATE STG.RPT_ETL_LAST_UPDATE SET CDC_UTC = SUBSELECT.ETL_LOAD_UTC
                    FROM (SELECT MAX(ETL_LOAD_UTC) ETL_LOAD_UTC FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW) SUBSELECT 
                    WHERE RPT_ETL_LAST_UPDATE.TASK = 'CLIENT_VIDEO_EVENT_RAW'`});                                   

    var alterTimeStampInput = snowflake.createStatement(
      {sqlText: "alter session set timestamp_input_format = 'DY MON DD YYYY'"});
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        updateClientVideoEventRawLastUpdate.execute();
        alterTimeStampInput.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var clientVideoEventRaw = snowflake.createStatement(
          {sqlText:`INSERT INTO DW_ODIN.CLIENT_VIDEO_EVENT_RAW
                          SELECT CLIENT_EVENT_FACT.APP_SID,
                          CLIENT_EVENT_FACT.APP_VERSION,
                          CLIENT_EVENT_FACT.CHANNEL_SID,
                          CLIENT_EVENT_FACT.CLIENT_GEO_SID,
                          CLIENT_EVENT_FACT.CONTENT_SESSION_SID,
                          CLIENT_EVENT_FACT.EVENT_OCCURRED_UTC,
                          CLIENT_EVENT_FACT.EVENT_TRANSACTION_ID,
                          CLIENT_EVENT_FACT.HIT_ID,
                          CLIENT_EVENT_FACT.PROGRAM_TIMELINE_ID,
                          CLIENT_EVENT_FACT.UTM_SID,
                          CLIENT_EVENT_FACT.ETL_LOAD_UTC,
                          EVENT_NAME_DIM.EVENT_NAME,
                          EVENT_NAME_DIM.EVENT_CATEGORY,
                          CLIENT_EVENT_FACT.USER_SID,
                          CLIENT_EVENT_FACT.CLIENT_IP_SID
                          FROM DW_ODIN.CLIENT_EVENT_FACT_LATEST_WEEK CLIENT_EVENT_FACT
                          JOIN DW_ODIN.EVENT_NAME_DIM ON CLIENT_EVENT_FACT.EVENT_NAME_SID = EVENT_NAME_DIM.EVENT_NAME_SID
                          WHERE EVENT_NAME_DIM.EVENT_NAME in('clipStart', 'clipEnd', 'cmPodBegin', 'cmPodEnd')
                          AND CLIENT_EVENT_FACT.EVENT_OCCURRED_UTC >= DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE))
                          AND CLIENT_EVENT_FACT.ETL_LOAD_UTC > (SELECT CDC_UTC FROM STG.RPT_ETL_LAST_UPDATE WHERE RPT_ETL_LAST_UPDATE.TASK = 'CLIENT_VIDEO_EVENT_RAW')`});
        res = clientVideoEventRaw.execute();
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
$$
;

CREATE OR REPLACE PROCEDURE "LOAD_CLIENT_HEARTBEAT_EVENT_RAW"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to insert data into CLIENT_HEARTBEAT_RAW that will store the events used to calculate the KPIs' FROM DUAL"}); 
    
    var updateClientHeartbeatRawLastUpdate = snowflake.createStatement(
      {sqlText: `UPDATE STG.RPT_ETL_LAST_UPDATE SET CDC_UTC = SUBSELECT.ETL_LOAD_UTC
                    FROM (SELECT MAX(ETL_LOAD_UTC) ETL_LOAD_UTC FROM DW_ODIN.CLIENT_HEARTBEAT_EVENT_RAW) SUBSELECT 
                    WHERE RPT_ETL_LAST_UPDATE.TASK = 'CLIENT_HEARTBEAT_EVENT_RAW'`});                                   

    var alterTimeStampInput = snowflake.createStatement(
      {sqlText: "alter session set timestamp_input_format = 'DY MON DD YYYY'"});
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        updateClientHeartbeatRawLastUpdate.execute();
        alterTimeStampInput.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var clientHeartbeatRaw = snowflake.createStatement(
          {sqlText:`INSERT INTO DW_ODIN.CLIENT_HEARTBEAT_EVENT_RAW
                      SELECT CLIENT_EVENT_FACT.APP_SID,
                      --CLIENT_EVENT_FACT.APP_VERSION,
                      --CLIENT_EVENT_FACT.CHANNEL_SID,
                      CLIENT_EVENT_FACT.CLIENT_GEO_SID,
                      CLIENT_EVENT_FACT.CONTENT_SESSION_SID,
                      --CLIENT_EVENT_FACT.EVENT_NAME_SID,
                      CLIENT_EVENT_FACT.EVENT_OCCURRED_UTC,
                      CLIENT_EVENT_FACT.EVENT_TRANSACTION_ID,
                      --CLIENT_EVENT_FACT.UTM_SID,
                      CLIENT_EVENT_FACT.ETL_LOAD_UTC
                      --EVENT_NAME_DIM.EVENT_NAME,
                      --EVENT_NAME_DIM.EVENT_CATEGORY
                      FROM DW_ODIN.CLIENT_EVENT_FACT_LATEST_WEEK CLIENT_EVENT_FACT
                      JOIN DW_ODIN.EVENT_NAME_DIM ON CLIENT_EVENT_FACT.EVENT_NAME_SID = EVENT_NAME_DIM.EVENT_NAME_SID
                      WHERE EVENT_NAME_DIM.EVENT_NAME = 'heartBeat'
                      AND CLIENT_EVENT_FACT.EVENT_OCCURRED_UTC >= DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE))
                      AND CLIENT_EVENT_FACT.ETL_LOAD_UTC > (SELECT CDC_UTC FROM STG.RPT_ETL_LAST_UPDATE WHERE RPT_ETL_LAST_UPDATE.TASK = 'CLIENT_HEARTBEAT_EVENT_RAW')`});
        res = clientHeartbeatRaw.execute();
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
$$    
;
    
CREATE OR REPLACE PROCEDURE "LOAD_CLIENT_LAST_HEARTBEAT_EVENT"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
$$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to merge into CLIENT_LAST_HEARTBEAT_EVENT used to calculate KPIs' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var clientLastHeartbeat = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CLIENT_LAST_HEARTBEAT_EVENT
                        USING ( select
                            CLIENT_GEO_BRIDGE.CLIENT_SID
                            ,CONTENT_SESSION_BRIDGE.SESSION_ID
                            ,max(EVENT_OCCURRED_UTC) as video_end_derived
                            from DW_ODIN.CLIENT_HEARTBEAT_EVENT_RAW CLIENT_HEARTBEAT_EVENT_RAW
                            JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_HEARTBEAT_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                            JOIN DW_ODIN.CONTENT_SESSION_BRIDGE CONTENT_SESSION_BRIDGE ON CLIENT_HEARTBEAT_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID

                            WHERE CLIENT_HEARTBEAT_EVENT_RAW.ETL_LOAD_UTC > (SELECT CDC_UTC FROM STG.RPT_ETL_LAST_UPDATE WHERE RPT_ETL_LAST_UPDATE.TASK = 'CLIENT_HEARTBEAT_EVENT_RAW')

                            group by CLIENT_GEO_BRIDGE.CLIENT_SID,CONTENT_SESSION_BRIDGE.SESSION_ID
                        ) STG_CLIENT_LAST_HEARTBEAT_EVENT
                        ON CLIENT_LAST_HEARTBEAT_EVENT.CLIENT_SID = STG_CLIENT_LAST_HEARTBEAT_EVENT.CLIENT_SID
                        AND CLIENT_LAST_HEARTBEAT_EVENT.SESSION_ID = STG_CLIENT_LAST_HEARTBEAT_EVENT.SESSION_ID
                        WHEN MATCHED AND CLIENT_LAST_HEARTBEAT_EVENT.video_end_derived < STG_CLIENT_LAST_HEARTBEAT_EVENT.video_end_derived
                        THEN UPDATE SET CLIENT_LAST_HEARTBEAT_EVENT.video_end_derived = STG_CLIENT_LAST_HEARTBEAT_EVENT.video_end_derived
                        WHEN NOT MATCHED THEN
                        INSERT (CLIENT_SID, SESSION_ID, VIDEO_END_DERIVED)
                        VALUES (CLIENT_SID, SESSION_ID, VIDEO_END_DERIVED)`});
        res = clientLastHeartbeat.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
        row_updated = res.getColumnValue(2);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, RECORDS_UPDATED = `
                      + row_updated
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
$$
  ;

--Need to modify for USER_SID change
CREATE OR REPLACE PROCEDURE "LOAD_CMS_MAPPING"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
$$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to insert data into CMS mapping tables that map the content ID to the CMS SID' FROM DUAL"});

    var beginTransaction = snowflake.createStatement(
      {sqlText: "BEGIN"});

    var commitTransaction = snowflake.createStatement(
      {sqlText: "COMMIT"});

     try {

        beginTransaction.execute();
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var updateChannelFlag = snowflake.createStatement(
          {sqlText:`UPDATE DW_ODIN.CMS_ODIN_CHANNEL_MAPPING SET CHANGE_FLAG = 'N'`});
        updateChannelFlag.execute();
        var updateChannelMapping = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CMS_ODIN_CHANNEL_MAPPING
                    USING (select CMS_CHANNEL_DIM.CMS_CHANNEL_SID, CHANNEL_DIM.CHANNEL_SID CHANNEL_SID, CHANNEL_DIM.CHANNEL_ID, CMS_CHANNEL_DIM.CHANNEL_ID CMS_CHANNEL_ID
                           FROM DW_ODIN.CHANNEL_DIM CHANNEL_DIM
                           LEFT JOIN DW_ODIN.CMS_CHANNEL_DIM ON IFNULL(CHANNEL_DIM.CHANNEL_ID, 'NA') = IFNULL(CMS_CHANNEL_DIM.CHANNEL_ID, 'NA')) STG_CMS_ODIN_CHANNEL_MAPPING
                            ON IFNULL(CMS_ODIN_CHANNEL_MAPPING.CHANNEL_ID, 'NA') = IFNULL(STG_CMS_ODIN_CHANNEL_MAPPING.CHANNEL_ID, 'NA')
                            WHEN MATCHED AND (CMS_ODIN_CHANNEL_MAPPING.CMS_CHANNEL_SID <> STG_CMS_ODIN_CHANNEL_MAPPING.CMS_CHANNEL_SID
                            OR CMS_ODIN_CHANNEL_MAPPING.CHANNEL_SID <> STG_CMS_ODIN_CHANNEL_MAPPING.CHANNEL_SID)
                            THEN UPDATE SET CMS_ODIN_CHANNEL_MAPPING.CMS_CHANNEL_SID = STG_CMS_ODIN_CHANNEL_MAPPING.CMS_CHANNEL_SID, 
                            CMS_ODIN_CHANNEL_MAPPING.CHANNEL_SID = STG_CMS_ODIN_CHANNEL_MAPPING.CHANNEL_SID, 
                            CHANGE_FLAG = 'Y'
                            WHEN NOT MATCHED THEN INSERT (CMS_CHANNEL_SID, CHANNEL_SID, CHANNEL_ID, CMS_CHANNEL_ID, CHANGE_FLAG)
                                                    VALUES (CMS_CHANNEL_SID, CHANNEL_SID, CHANNEL_ID, CMS_CHANNEL_ID, 'N')`});
        res1 = updateChannelMapping.execute();
        res1.next();
        row_inserted1 = res1.getColumnValue(1);
        row_updated1 = res1.getColumnValue(2);

        var updateClipFlag = snowflake.createStatement(
          {sqlText:`UPDATE DW_ODIN.CMS_ODIN_CLIP_MAPPING SET CHANGE_FLAG = 'N'`});
        updateClipFlag.execute();
        var updateClipMapping = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CMS_ODIN_CLIP_MAPPING
                      USING (select CMS_CLIP_DIM.CMS_CLIP_SID, CLIP_DIM.CLIP_SID CLIP_SID, CLIP_DIM.CLIP_ID, CMS_CLIP_DIM.CLIP_ID CMS_CLIP_ID
                              FROM DW_ODIN.CLIP_DIM CLIP_DIM
                              LEFT JOIN DW_ODIN.CMS_CLIP_DIM ON IFNULL(CLIP_DIM.CLIP_ID, 'NA') = IFNULL(CMS_CLIP_DIM.CLIP_ID, 'NA')) STG_CMS_ODIN_CLIP_MAPPING
                              ON IFNULL(CMS_ODIN_CLIP_MAPPING.CLIP_ID, 'NA') = IFNULL(STG_CMS_ODIN_CLIP_MAPPING.CLIP_ID, 'NA')
                              WHEN MATCHED AND (CMS_ODIN_CLIP_MAPPING.CMS_CLIP_SID <> STG_CMS_ODIN_CLIP_MAPPING.CMS_CLIP_SID
                              OR CMS_ODIN_CLIP_MAPPING.CLIP_SID <> STG_CMS_ODIN_CLIP_MAPPING.CLIP_SID)
                              THEN UPDATE SET CMS_ODIN_CLIP_MAPPING.CMS_CLIP_SID = STG_CMS_ODIN_CLIP_MAPPING.CMS_CLIP_SID, 
                              CMS_ODIN_CLIP_MAPPING.CLIP_SID = STG_CMS_ODIN_CLIP_MAPPING.CLIP_SID, 
                              CHANGE_FLAG = 'Y'
                              WHEN NOT MATCHED THEN INSERT (CMS_CLIP_SID, CLIP_SID, CLIP_ID, CMS_CLIP_ID, CHANGE_FLAG)
                                                      VALUES (CMS_CLIP_SID, CLIP_SID, CLIP_ID, CMS_CLIP_ID, 'N')`});
        res2 = updateClipMapping.execute();    
        res2.next();
        row_inserted2 = res2.getColumnValue(1);
        row_updated2 = res2.getColumnValue(2);

        var updateEpisodeFlag = snowflake.createStatement(
          {sqlText:`UPDATE DW_ODIN.CMS_ODIN_EPISODE_MAPPING SET CHANGE_FLAG = 'N'`});
        updateEpisodeFlag.execute();
        var updateEpisodeMapping = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CMS_ODIN_EPISODE_MAPPING
                      USING (select CMS_EPISODE_DIM.CMS_EPISODE_SID, EPISODE_DIM.EPISODE_SID EPISODE_SID, EPISODE_DIM.EPISODE_ID, CMS_EPISODE_DIM.EPISODE_ID CMS_EPISODE_ID
                              FROM DW_ODIN.EPISODE_DIM EPISODE_DIM
                              LEFT JOIN DW_ODIN.CMS_EPISODE_DIM ON IFNULL(EPISODE_DIM.EPISODE_ID, 'NA') = IFNULL(CMS_EPISODE_DIM.EPISODE_ID, 'NA')) STG_CMS_ODIN_EPISODE_MAPPING
                              ON IFNULL(CMS_ODIN_EPISODE_MAPPING.EPISODE_ID, 'NA') = IFNULL(STG_CMS_ODIN_EPISODE_MAPPING.EPISODE_ID, 'NA')
                              WHEN MATCHED AND (CMS_ODIN_EPISODE_MAPPING.CMS_EPISODE_SID <> STG_CMS_ODIN_EPISODE_MAPPING.CMS_EPISODE_SID
                              OR CMS_ODIN_EPISODE_MAPPING.EPISODE_SID <> STG_CMS_ODIN_EPISODE_MAPPING.EPISODE_SID)
                              THEN UPDATE SET CMS_ODIN_EPISODE_MAPPING.CMS_EPISODE_SID = STG_CMS_ODIN_EPISODE_MAPPING.CMS_EPISODE_SID, 
                              CMS_ODIN_EPISODE_MAPPING.EPISODE_SID = STG_CMS_ODIN_EPISODE_MAPPING.EPISODE_SID, 
                              CHANGE_FLAG = 'Y'
                              WHEN NOT MATCHED THEN INSERT (CMS_EPISODE_SID, EPISODE_SID, EPISODE_ID, CMS_EPISODE_ID, CHANGE_FLAG)
                                                      VALUES (CMS_EPISODE_SID, EPISODE_SID, EPISODE_ID, CMS_EPISODE_ID, 'N')`});
        res3 = updateEpisodeMapping.execute(); 
        res3.next();
        row_inserted3 = res3.getColumnValue(1);
        row_updated3 = res3.getColumnValue(2);
        
        var updateTimelineFlag = snowflake.createStatement(
          {sqlText:`UPDATE DW_ODIN.CMS_ODIN_TIMELINE_MAPPING SET CHANGE_FLAG = 'N'`});
        updateTimelineFlag.execute();
        var updateTimelineMapping = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CMS_ODIN_TIMELINE_MAPPING
                      USING (select CMS_TIMELINE_DIM.CMS_TIMELINE_SID, TIMELINE_DIM.PROGRAM_TIMELINE_ID TIMELINE_ID, CMS_TIMELINE_DIM.TIMELINE_ID CMS_TIMELINE_ID
                            FROM DW_ODIN.CLIENT_EVENT_FACT_PROGRAM_TIMELINE_ID TIMELINE_DIM
                            LEFT JOIN DW_ODIN.CMS_TIMELINE_DIM CMS_TIMELINE_DIM ON IFNULL(TIMELINE_DIM.PROGRAM_TIMELINE_ID, 'NA') = IFNULL(CMS_TIMELINE_DIM.TIMELINE_ID, 'NA')) STG_CMS_ODIN_TIMELINE_MAPPING
                              ON IFNULL(CMS_ODIN_TIMELINE_MAPPING.TIMELINE_ID, 'NA') = IFNULL(STG_CMS_ODIN_TIMELINE_MAPPING.TIMELINE_ID, 'NA')
                              WHEN MATCHED AND CMS_ODIN_TIMELINE_MAPPING.CMS_TIMELINE_SID <> STG_CMS_ODIN_TIMELINE_MAPPING.CMS_TIMELINE_SID
                              THEN UPDATE SET CMS_ODIN_TIMELINE_MAPPING.CMS_TIMELINE_SID = STG_CMS_ODIN_TIMELINE_MAPPING.CMS_TIMELINE_SID, 
                              CHANGE_FLAG = 'Y'
                              WHEN NOT MATCHED THEN INSERT (CMS_TIMELINE_SID, TIMELINE_ID, CMS_TIMELINE_ID, CHANGE_FLAG)
                                                      VALUES (CMS_TIMELINE_SID, TIMELINE_ID, CMS_TIMELINE_ID, 'N')`});
        res4 = updateTimelineMapping.execute();
        res4.next();
        row_inserted4 = res4.getColumnValue(1);
        row_updated4 = res4.getColumnValue(2);

        var updateUserFlag = snowflake.createStatement(
          {sqlText:`UPDATE DW_ODIN.CMS_ODIN_USER_MAPPING SET CHANGE_FLAG = 'N'`});
        updateUserFlag.execute();
        var updateUserMapping = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CMS_ODIN_USER_MAPPING
                    USING (select CMS_USER_DIM.CMS_USER_SID, USER_DIM.USER_SID USER_SID, USER_DIM.USER_ID, CMS_USER_DIM.USER_ID CMS_USER_ID
                           FROM DW_ODIN.USER_DIM_VW USER_DIM
                           LEFT JOIN DW_ODIN.CMS_USER_DIM_VW CMS_USER_DIM ON IFNULL(USER_DIM.USER_ID, 'NA') = IFNULL(CMS_USER_DIM.USER_ID, 'NA')) STG_CMS_ODIN_USER_MAPPING
                            ON IFNULL(CMS_ODIN_USER_MAPPING.USER_ID, 'NA') = IFNULL(STG_CMS_ODIN_USER_MAPPING.USER_ID, 'NA')
                            WHEN MATCHED AND (CMS_ODIN_USER_MAPPING.CMS_USER_SID <> STG_CMS_ODIN_USER_MAPPING.CMS_USER_SID
                            OR CMS_ODIN_USER_MAPPING.USER_SID <> STG_CMS_ODIN_USER_MAPPING.USER_SID)
                            THEN UPDATE SET CMS_ODIN_USER_MAPPING.CMS_USER_SID = STG_CMS_ODIN_USER_MAPPING.CMS_USER_SID, 
                            CMS_ODIN_USER_MAPPING.USER_SID = STG_CMS_ODIN_USER_MAPPING.USER_SID, 
                            CHANGE_FLAG = 'Y'
                            WHEN NOT MATCHED THEN INSERT (CMS_USER_SID, USER_SID, USER_ID, CMS_USER_ID, CHANGE_FLAG)
                                                    VALUES (CMS_USER_SID, USER_SID, USER_ID, CMS_USER_ID, 'N')`});
        res1 = updateUserMapping.execute();
        res1.next();
        row_inserted5 = res1.getColumnValue(1);
        row_updated5 = res1.getColumnValue(2);
        
        row_inserted = row_inserted1 + row_inserted2 + row_inserted3 + row_inserted4 + row_inserted5;
        row_updated = row_updated1 + row_updated2 + row_updated3 + row_updated4 + row_updated4;
        
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = `
                      + row_inserted
                      + `, RECORDS_UPDATED = `
                      + row_updated 
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2) || ' | ' || LAST_QUERY_ID(-3) || ' | ' || LAST_QUERY_ID(-4) || ' | ' || LAST_QUERY_ID(-4))  `});
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
$$
;

--Need to modify for USER_SID change
CREATE OR REPLACE PROCEDURE "LOAD_CLIENT_SESSION_TO_BE_UPDATED"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
$$
  
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to insert CLIENT ID and SESSION ID that need to be changed' FROM DUAL"});
          
    try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var CLIENT_SESSION_TO_BE_UPDATED = snowflake.createStatement(
          {sqlText:`--Get a list of client ids and session ids to be updated
                            --Get all the clients and sessions that were last updated after the last update variable
                            CREATE OR REPLACE TRANSIENT TABLE DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED AS (
                                SELECT CLIENT_SID, SESSION_ID 
                                FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW CLIENT_VIDEO_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                WHERE CLIENT_VIDEO_EVENT_RAW.ETL_LOAD_UTC > (SELECT CDC_UTC FROM STG.RPT_ETL_LAST_UPDATE WHERE RPT_ETL_LAST_UPDATE.TASK = 'CLIENT_VIDEO_EVENT_RAW')
                                --WHERE CLIENT_VIDEO_EVENT_RAW.ETL_LOAD_UTC > DATEADD('day', -7, CURRENT_TIMESTAMP())

                                UNION

                                --Get all the clients and sessions that were added in the last day, but with the event occurred date after the ETL load date (data issue)
                                SELECT CLIENT_SID, SESSION_ID
                                FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW CLIENT_VIDEO_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                WHERE CLIENT_VIDEO_EVENT_RAW.ETL_LOAD_UTC >= (SELECT DATEADD('day', -1, CURRENT_DATE())) AND
                                CLIENT_VIDEO_EVENT_RAW.EVENT_OCCURRED_UTC >= CLIENT_VIDEO_EVENT_RAW.ETL_LOAD_UTC

                                UNION

                                SELECT CLIENT_SID, SESSION_ID
                                FROM DW_ODIN.CLIENT_VIDEO_SEGMENTS CLIENT_VIDEO_SEGMENTS
                                WHERE CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC >= (SELECT DATEADD('day', -1, CURRENT_DATE()))

                                UNION


                                SELECT CLIENT_SID, SESSION_ID 
                                FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                JOIN DW_ODIN.CHANNEL_DIM ON CLIENT_VIDEO_EVENT_RAW.CHANNEL_SID = CHANNEL_DIM.CHANNEL_SID
                                JOIN DW_ODIN.CMS_ODIN_CHANNEL_MAPPING ON CMS_ODIN_CHANNEL_MAPPING.CHANNEL_ID = CHANNEL_DIM.CHANNEL_ID
                                WHERE CMS_ODIN_CHANNEL_MAPPING.CHANGE_FLAG = 'Y'

                                UNION

                                SELECT CLIENT_SID, SESSION_ID 
                                FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                JOIN DW_ODIN.EPISODE_DIM ON CONTENT_SESSION_BRIDGE.EPISODE_SID = EPISODE_DIM.EPISODE_SID
                                JOIN DW_ODIN.CMS_ODIN_EPISODE_MAPPING ON CMS_ODIN_EPISODE_MAPPING.EPISODE_ID = EPISODE_DIM.EPISODE_ID
                                WHERE CMS_ODIN_EPISODE_MAPPING.CHANGE_FLAG = 'Y' 


                                UNION

                                SELECT CLIENT_SID, SESSION_ID 
                                FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                JOIN DW_ODIN.CLIP_DIM ON CONTENT_SESSION_BRIDGE.CLIP_SID = CLIP_DIM.CLIP_SID
                                JOIN DW_ODIN.CMS_ODIN_CLIP_MAPPING ON CMS_ODIN_CLIP_MAPPING.CLIP_ID = CLIP_DIM.CLIP_ID
                                WHERE CMS_ODIN_CLIP_MAPPING.CHANGE_FLAG = 'Y'

                                UNION

                                SELECT CLIENT_SID, SESSION_ID 
                                FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                JOIN DW_ODIN.CMS_ODIN_TIMELINE_MAPPING ON CMS_ODIN_TIMELINE_MAPPING.TIMELINE_ID = CLIENT_VIDEO_EVENT_RAW.PROGRAM_TIMELINE_ID
                                WHERE CMS_ODIN_TIMELINE_MAPPING.CHANGE_FLAG = 'Y'

                                UNION


                                SELECT CLIENT_SID, SESSION_ID 
                                FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                JOIN DW_ODIN.USER_DIM_VW USER_DIM ON CLIENT_VIDEO_EVENT_RAW.USER_SID = USER_DIM.USER_SID
                                JOIN DW_ODIN.CMS_ODIN_USER_MAPPING ON CMS_ODIN_USER_MAPPING.USER_ID = USER_DIM.USER_ID
                                WHERE CMS_ODIN_USER_MAPPING.CHANGE_FLAG = 'Y'

                                UNION

                                SELECT CLIENT_SID, SESSION_ID 
                                FROM DW_ODIN.CLIENT_HEARTBEAT_EVENT_RAW
                                JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_HEARTBEAT_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                                JOIN DW_ODIN.CONTENT_SESSION_BRIDGE CONTENT_SESSION_BRIDGE ON CLIENT_HEARTBEAT_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                                WHERE CLIENT_HEARTBEAT_EVENT_RAW.ETL_LOAD_UTC > (SELECT CDC_UTC FROM STG.RPT_ETL_LAST_UPDATE WHERE RPT_ETL_LAST_UPDATE.TASK = 'CLIENT_HEARTBEAT_EVENT_RAW')
                                --WHERE S_CLIENT_LAST_HEARTBEAT_EVENT.video_end_derived > DATEADD('day', -7, CURRENT_TIMESTAMP())  
                                )`});
        CLIENT_SESSION_TO_BE_UPDATED.execute(); 
       
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = (select count(*) from DW_ODIN.S_CLIENT_SESSION_TO_BE_UPDATED)
                      , STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1))  `});
        updateEtlBatchAuditSuccess.execute();
                    
        return "Succeeded. ";
        
        }
        
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
$$
;

--Need to modify for USER_SID change
CREATE OR REPLACE PROCEDURE "LOAD_CLIENT_VIDEO_SEGMENT_LENGTH"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    

    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to merge into CLIENT_VIDEO_SEGMENT_LENGTH used to calculate KPIs' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var etlLoad0 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE SUBSELECT AS 
(select DISTINCT
                        CLIENT_VIDEO_EVENT_RAW.APP_SID
                        , CLIENT_VIDEO_EVENT_RAW.APP_VERSION  
                        , IFNULL(CMS_ODIN_CHANNEL_MAPPING.CMS_CHANNEL_SID, 0) CMS_CHANNEL_SID
                        , CASE WHEN CMS_ODIN_CHANNEL_MAPPING.CHANNEL_ID IS NULL THEN 'NA'
                                WHEN CMS_ODIN_CHANNEL_MAPPING.CMS_CHANNEL_ID IS NULL THEN 'Unknown'
                                ELSE CMS_ODIN_CHANNEL_MAPPING.CMS_CHANNEL_ID END CMS_CHANNEL_ID
                        , IFNULL(CMS_ODIN_CLIP_MAPPING.CMS_CLIP_SID, 0) CMS_CLIP_SID
                        , CASE WHEN CMS_ODIN_CLIP_MAPPING.CLIP_ID IS NULL THEN 'NA'
                                WHEN CMS_ODIN_CLIP_MAPPING.CMS_CLIP_ID IS NULL THEN 'Unknown'
                                ELSE CMS_ODIN_CLIP_MAPPING.CMS_CLIP_ID END CMS_CLIP_ID
                        , IFNULL(CMS_ODIN_EPISODE_MAPPING.CMS_EPISODE_SID, 0) CMS_EPISODE_SID
                        , CASE WHEN CMS_ODIN_EPISODE_MAPPING.EPISODE_ID IS NULL THEN 'NA'
                                WHEN CMS_ODIN_EPISODE_MAPPING.CMS_EPISODE_ID IS NULL THEN 'Unknown'
                                ELSE CMS_ODIN_EPISODE_MAPPING.CMS_EPISODE_ID END CMS_EPISODE_ID
                        , IFNULL(CMS_ODIN_TIMELINE_MAPPING.CMS_TIMELINE_SID, 0) CMS_TIMELINE_SID
                        , CASE WHEN CMS_ODIN_TIMELINE_MAPPING.TIMELINE_ID IS NULL THEN 'NA'
                                WHEN CMS_ODIN_TIMELINE_MAPPING.CMS_TIMELINE_ID IS NULL THEN 'Unknown'
                                ELSE CMS_ODIN_TIMELINE_MAPPING.CMS_TIMELINE_ID END CMS_TIMELINE_ID
                        , IFNULL(CMS_ODIN_USER_MAPPING.CMS_USER_SID, 0) CMS_USER_SID
                        , CASE WHEN CMS_ODIN_USER_MAPPING.USER_ID IS NULL THEN 'NA'
                                WHEN CMS_ODIN_USER_MAPPING.CMS_USER_ID IS NULL THEN 'Unknown'
                                ELSE CMS_ODIN_USER_MAPPING.CMS_USER_ID END CMS_USER_ID
                        , CLIENT_VIDEO_EVENT_RAW.HIT_ID
                        , CLIENT_VIDEO_EVENT_RAW.EVENT_NAME
                        , CONTENT_SESSION_BRIDGE.SESSION_ID
                        , CLIENT_GEO_BRIDGE.CLIENT_SID
                        , CLIENT_GEO_BRIDGE.GEO_SID
                        , CLIENT_VIDEO_EVENT_RAW.UTM_SID
                        , case when CLIENT_VIDEO_EVENT_RAW.EVENT_CATEGORY = 'watch' then 'content' else 'ad' end as video_segment_type
                        , CLIENT_VIDEO_EVENT_RAW.EVENT_OCCURRED_UTC VIDEO_SEGMENT_BEGIN_UTC
                        FROM DW_ODIN.CLIENT_VIDEO_EVENT_RAW CLIENT_VIDEO_EVENT_RAW
                        JOIN DW_ODIN.CLIENT_GEO_BRIDGE CLIENT_GEO_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CLIENT_GEO_SID = CLIENT_GEO_BRIDGE.CLIENT_GEO_SID
                        JOIN DW_ODIN.CONTENT_SESSION_BRIDGE CONTENT_SESSION_BRIDGE ON CLIENT_VIDEO_EVENT_RAW.CONTENT_SESSION_SID = CONTENT_SESSION_BRIDGE.CONTENT_SESSION_SID
                        JOIN DW_ODIN.CMS_ODIN_CHANNEL_MAPPING CMS_ODIN_CHANNEL_MAPPING ON CLIENT_VIDEO_EVENT_RAW.CHANNEL_SID = CMS_ODIN_CHANNEL_MAPPING.CHANNEL_SID
                        JOIN DW_ODIN.CMS_ODIN_CLIP_MAPPING CMS_ODIN_CLIP_MAPPING ON CONTENT_SESSION_BRIDGE.CLIP_SID = CMS_ODIN_CLIP_MAPPING.CLIP_SID
                        JOIN DW_ODIN.CMS_ODIN_EPISODE_MAPPING CMS_ODIN_EPISODE_MAPPING ON CONTENT_SESSION_BRIDGE.EPISODE_SID = CMS_ODIN_EPISODE_MAPPING.EPISODE_SID
                        JOIN DW_ODIN.CMS_ODIN_TIMELINE_MAPPING CMS_ODIN_TIMELINE_MAPPING ON IFNULL(CLIENT_VIDEO_EVENT_RAW.PROGRAM_TIMELINE_ID, 'NA') = IFNULL(CMS_ODIN_TIMELINE_MAPPING.TIMELINE_ID, 'NA')
                        JOIN DW_ODIN.CMS_ODIN_USER_MAPPING CMS_ODIN_USER_MAPPING ON CLIENT_VIDEO_EVENT_RAW.USER_SID = CMS_ODIN_USER_MAPPING.USER_SID
                        JOIN DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED ON IFNULL(CONTENT_SESSION_BRIDGE.SESSION_ID, 'NA') = IFNULL(CLIENT_SESSION_TO_BE_UPDATED.SESSION_ID, 'NA') AND 
                             CLIENT_GEO_BRIDGE.CLIENT_SID = CLIENT_SESSION_TO_BE_UPDATED.CLIENT_SID
                      )`});
        etlLoad0.execute();

        var etlLoad = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CLIENT_VIDEO_SEGMENT_LENGTH CLIENT_VIDEO_SEGMENT_LENGTH
                    USING (SELECT SUBSELECT.*
                        , lead(VIDEO_SEGMENT_BEGIN_UTC) 
                            over(partition by CLIENT_SID, SESSION_ID
                                 order by VIDEO_SEGMENT_BEGIN_UTC ASC, HIT_ID ASC, 
                                 GEO_SID ASC, APP_SID ASC, CMS_CHANNEL_SID ASC, CMS_EPISODE_SID ASC, CMS_CLIP_SID ASC, cms_timeline_sid asc, CMS_USER_SID ASC) as video_segment_end_UTC
                        , current_timestamp::timestamp_ntz AS ETL_LOAD_UTC
                    FROM SUBSELECT
                    ) STG_CLIENT_VIDEO_SEGMENT_LENGTH
                    ON CLIENT_VIDEO_SEGMENT_LENGTH.APP_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.APP_SID
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.APP_VERSION, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.APP_VERSION, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_ID, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CLIP_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CLIP_ID, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.CMS_EPISODE_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_EPISODE_ID, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_ID, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.CMS_USER_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_USER_ID, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.HIT_ID, -1) = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.HIT_ID, -1)
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.EVENT_NAME, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.EVENT_NAME, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.SESSION_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.SESSION_ID, 'NA')
                    AND CLIENT_VIDEO_SEGMENT_LENGTH.CLIENT_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.CLIENT_SID
                    AND CLIENT_VIDEO_SEGMENT_LENGTH.GEO_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.GEO_SID
                    AND CLIENT_VIDEO_SEGMENT_LENGTH.UTM_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.UTM_SID
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.video_segment_type, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.video_segment_type, 'NA')
                    AND IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.VIDEO_SEGMENT_BEGIN_UTC, '1900-01-01') = IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.VIDEO_SEGMENT_BEGIN_UTC, '1900-01-01')
                    WHEN MATCHED 
                    AND  (IFNULL(CLIENT_VIDEO_SEGMENT_LENGTH.video_segment_end_UTC, '1900-01-01') <> IFNULL(STG_CLIENT_VIDEO_SEGMENT_LENGTH.video_segment_end_UTC, '1900-01-01')
                    OR CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_SID <> STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_SID
                    OR CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CLIP_SID <> STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CLIP_SID
                    OR CLIENT_VIDEO_SEGMENT_LENGTH.CMS_EPISODE_SID <> STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_EPISODE_SID
                    OR CLIENT_VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_SID <> STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_SID
                    OR CLIENT_VIDEO_SEGMENT_LENGTH.CMS_USER_SID <> STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_USER_SID
                    )
                    THEN UPDATE SET CLIENT_VIDEO_SEGMENT_LENGTH.video_segment_end_UTC = STG_CLIENT_VIDEO_SEGMENT_LENGTH.video_segment_end_UTC,
                                    CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_SID,
                                    CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CLIP_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_CLIP_SID,
                                    CLIENT_VIDEO_SEGMENT_LENGTH.CMS_EPISODE_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_EPISODE_SID,
                                    CLIENT_VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_SID,
                                    CLIENT_VIDEO_SEGMENT_LENGTH.CMS_USER_SID = STG_CLIENT_VIDEO_SEGMENT_LENGTH.CMS_USER_SID,
                                    ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                    WHEN NOT MATCHED THEN INSERT (APP_SID, APP_VERSION, CMS_CHANNEL_SID, CMS_CHANNEL_ID, CMS_CLIP_SID, CMS_CLIP_ID, CMS_EPISODE_SID, CMS_EPISODE_ID, CMS_TIMELINE_SID, CMS_TIMELINE_ID, CMS_USER_SID, CMS_USER_ID,
                                                  HIT_ID, EVENT_NAME, SESSION_ID, CLIENT_SID, GEO_SID, UTM_SID, VIDEO_SEGMENT_TYPE, 
                                                  VIDEO_SEGMENT_BEGIN_UTC, VIDEO_SEGMENT_END_UTC, ETL_LOAD_UTC)
                                          VALUES (APP_SID, APP_VERSION, CMS_CHANNEL_SID, CMS_CHANNEL_ID, CMS_CLIP_SID, CMS_CLIP_ID, CMS_EPISODE_SID, CMS_EPISODE_ID, CMS_TIMELINE_SID, CMS_TIMELINE_ID, CMS_USER_SID, CMS_USER_ID,
                                                  HIT_ID, EVENT_NAME, SESSION_ID, CLIENT_SID, GEO_SID, UTM_SID, VIDEO_SEGMENT_TYPE, 
                                                  VIDEO_SEGMENT_BEGIN_UTC, VIDEO_SEGMENT_END_UTC, current_timestamp::timestamp_ntz)`});
        res = etlLoad.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
        row_updated = res.getColumnValue(2);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, RECORDS_UPDATED = `
                      + row_updated
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
    
        
    $$
    ;

--Need to modify for USER_SID change
CREATE OR REPLACE PROCEDURE "LOAD_CLIENT_VIDEO_SEGMENTS"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to merge into CLIENT_VIDEO_SEGMENTS used to calculate KPIs' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);
        
        var etlLoad0 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE RPT.SP_CRICKET_MAPPING_SID
                      AS
                      SELECT CLIENT_DIM.CLIENT_SID
                      FROM (SELECT DISTINCT CLIENT_ID FROM PUBLIC.SP_CRICKET_MAPPING) SP_CRICKET_MAPPING
                      JOIN DW_ODIN.CLIENT_DIM ON SP_CRICKET_MAPPING.CLIENT_ID = CLIENT_DIM.CLIENT_ID`});
        etlLoad0.execute();
        
        var setVar = snowflake.createStatement(
          {sqlText:`SET CRICKET_APP_SID = (SELECT TOP 1 APP_SID FROM DW_ODIN.APP_DIM WHERE APP_NAME = 'cricket' AND APP_ID = '1k')`});
        setVar.execute();
                
        var etlLoad = snowflake.createStatement(
          {sqlText:`MERGE INTO DW_ODIN.CLIENT_VIDEO_SEGMENTS CLIENT_VIDEO_SEGMENTS
                        USING
                        (
                            select DISTINCT
                            CASE WHEN SP_CRICKET_MAPPING_SID.CLIENT_SID IS NOT NULL THEN $CRICKET_APP_SID ELSE VIDEO_SEGMENT_LENGTH.APP_SID END APP_SID
                            , VIDEO_SEGMENT_LENGTH.APP_VERSION
                            ,VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_SID
                            ,VIDEO_SEGMENT_LENGTH.CMS_CHANNEL_ID
                            ,VIDEO_SEGMENT_LENGTH.CMS_CLIP_SID
                            ,VIDEO_SEGMENT_LENGTH.CMS_CLIP_ID
                            ,VIDEO_SEGMENT_LENGTH.CMS_EPISODE_SID
                            ,VIDEO_SEGMENT_LENGTH.CMS_EPISODE_ID
                            ,VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_SID
                            ,VIDEO_SEGMENT_LENGTH.CMS_TIMELINE_ID
                            ,VIDEO_SEGMENT_LENGTH.CMS_USER_SID
                            ,VIDEO_SEGMENT_LENGTH.CMS_USER_ID
                            ,VIDEO_SEGMENT_LENGTH.HIT_ID
                            ,VIDEO_SEGMENT_LENGTH.SESSION_ID
                            ,VIDEO_SEGMENT_LENGTH.CLIENT_SID
                            ,VIDEO_SEGMENT_LENGTH.GEO_SID
                            ,VIDEO_SEGMENT_LENGTH.UTM_SID
                            ,VIDEO_SEGMENT_LENGTH.VIDEO_SEGMENT_TYPE
                            ,VIDEO_SEGMENT_LENGTH.VIDEO_SEGMENT_BEGIN_UTC
                            ,coalesce(video_segment_length.video_segment_end_UTC, 
                                      greatest(VIDEO_SEGMENT_LENGTH.VIDEO_SEGMENT_BEGIN_UTC, IFNULL(last_video_event.video_end_derived, '1900-01-01')), 
                                      VIDEO_SEGMENT_LENGTH.VIDEO_SEGMENT_BEGIN_UTC) as VIDEO_SEGMENT_END_UTC
                            ,current_timestamp::timestamp_ntz ETL_LOAD_UTC
                            from DW_ODIN.CLIENT_VIDEO_SEGMENT_LENGTH as VIDEO_SEGMENT_LENGTH
                          
                            LEFT JOIN RPT.SP_CRICKET_MAPPING_SID ON VIDEO_SEGMENT_LENGTH.CLIENT_SID = SP_CRICKET_MAPPING_SID.CLIENT_SID

                            JOIN DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED ON VIDEO_SEGMENT_LENGTH.SESSION_ID = CLIENT_SESSION_TO_BE_UPDATED.SESSION_ID
                                AND VIDEO_SEGMENT_LENGTH.CLIENT_SID = CLIENT_SESSION_TO_BE_UPDATED.CLIENT_SID

                            LEFT JOIN DW_ODIN.CLIENT_LAST_HEARTBEAT_EVENT as LAST_VIDEO_EVENT
                            on VIDEO_SEGMENT_LENGTH.CLIENT_SID = LAST_VIDEO_EVENT.CLIENT_SID
                            and IFNULL(VIDEO_SEGMENT_LENGTH.session_id, 'NA') = IFNULL(LAST_VIDEO_EVENT.session_id, 'NA') 
                            WHERE VIDEO_SEGMENT_LENGTH.EVENT_NAME in ('clipStart', 'cmPodBegin')

                        ) STG_CLIENT_VIDEO_SEGMENTS
                        ON CLIENT_VIDEO_SEGMENTS.APP_SID = STG_CLIENT_VIDEO_SEGMENTS.APP_SID
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.APP_VERSION, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.APP_VERSION, 'NA')
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_ID, 'NA')
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.CMS_CLIP_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.CMS_CLIP_ID, 'NA')
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_ID, 'NA')
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_ID, 'NA')
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.CMS_USER_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.CMS_USER_ID, 'NA')
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.HIT_ID, -1) = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.HIT_ID, -1)
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.SESSION_ID, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.SESSION_ID, 'NA')
                        AND CLIENT_VIDEO_SEGMENTS.CLIENT_SID = STG_CLIENT_VIDEO_SEGMENTS.CLIENT_SID
                        AND CLIENT_VIDEO_SEGMENTS.GEO_SID = STG_CLIENT_VIDEO_SEGMENTS.GEO_SID
                        AND CLIENT_VIDEO_SEGMENTS.UTM_SID = STG_CLIENT_VIDEO_SEGMENTS.UTM_SID
                        AND IFNULL(CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(STG_CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_TYPE, 'NA')
                        AND CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_BEGIN_UTC = STG_CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_BEGIN_UTC
                        WHEN MATCHED 
                        AND (CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC <> STG_CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC
                        OR CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_SID <> STG_CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_SID
                        OR CLIENT_VIDEO_SEGMENTS.CMS_CLIP_SID <> STG_CLIENT_VIDEO_SEGMENTS.CMS_CLIP_SID
                        OR CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_SID <> STG_CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_SID
                        OR CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_SID <> STG_CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_SID
                        OR CLIENT_VIDEO_SEGMENTS.CMS_USER_SID <> STG_CLIENT_VIDEO_SEGMENTS.CMS_USER_SID
                        )
                        THEN UPDATE SET CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC = STG_CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC,
                                        CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_SID = STG_CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_SID,
                                        CLIENT_VIDEO_SEGMENTS.CMS_CLIP_SID = STG_CLIENT_VIDEO_SEGMENTS.CMS_CLIP_SID,
                                        CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_SID = STG_CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_SID,
                                        CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_SID = STG_CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_SID,
                                        CLIENT_VIDEO_SEGMENTS.CMS_USER_SID = STG_CLIENT_VIDEO_SEGMENTS.CMS_USER_SID,
                                        CLIENT_VIDEO_SEGMENTS.ETL_LOAD_UTC = current_timestamp::timestamp_ntz 
                        WHEN NOT MATCHED
                        THEN INSERT (APP_SID, APP_VERSION, CMS_CHANNEL_SID, CMS_CHANNEL_ID, CMS_CLIP_SID, CMS_CLIP_ID, CMS_EPISODE_SID, CMS_EPISODE_ID, 
                                     CMS_TIMELINE_SID, CMS_TIMELINE_ID, CMS_USER_SID, CMS_USER_ID, HIT_ID, SESSION_ID, CLIENT_SID, GEO_SID, UTM_SID, 
                                     VIDEO_SEGMENT_TYPE, VIDEO_SEGMENT_BEGIN_UTC, VIDEO_SEGMENT_END_UTC, ETL_LOAD_UTC)
                        VALUES (APP_SID, APP_VERSION, CMS_CHANNEL_SID, CMS_CHANNEL_ID, CMS_CLIP_SID, CMS_CLIP_ID, CMS_EPISODE_SID, CMS_EPISODE_ID, 
                                CMS_TIMELINE_SID, CMS_TIMELINE_ID, CMS_USER_SID, CMS_USER_ID, HIT_ID, SESSION_ID, CLIENT_SID, GEO_SID, UTM_SID, 
                                VIDEO_SEGMENT_TYPE, VIDEO_SEGMENT_BEGIN_UTC, VIDEO_SEGMENT_END_UTC, current_timestamp::timestamp_ntz)`});
        res = etlLoad.execute();
        res.next();
        row_inserted = res.getColumnValue(1);
        row_updated = res.getColumnValue(2);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, RECORDS_UPDATED = `
                      + row_updated
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
    $$
    ;

CREATE OR REPLACE PROCEDURE "UPDATE_CRICKET_MAPPING"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update CLIENT_VIDEO_SEGMENTS with the CRICKET_MAPPING' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE RPT.SP_CRICKET_MAPPING_SID
                      AS
                      SELECT CLIENT_DIM.CLIENT_SID
                      FROM (SELECT DISTINCT CLIENT_ID FROM PUBLIC.SP_CRICKET_MAPPING) SP_CRICKET_MAPPING
                      JOIN DW_ODIN.CLIENT_DIM ON SP_CRICKET_MAPPING.CLIENT_ID = CLIENT_DIM.CLIENT_ID`});
        etlLoad.execute();
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`UPDATE DW_ODIN.CLIENT_VIDEO_SEGMENTS
                    SET CLIENT_VIDEO_SEGMENTS.APP_SID = CRICKET_APP.APP_SID
                    FROM (SELECT TOP 1 APP_SID FROM DW_ODIN.APP_DIM WHERE APP_NAME = 'cricket' AND APP_ID = '1k') CRICKET_APP
                    WHERE CLIENT_VIDEO_SEGMENTS.CLIENT_SID IN (SELECT CLIENT_SID FROM RPT.SP_CRICKET_MAPPING_SID)
                    AND CLIENT_VIDEO_SEGMENTS.APP_SID <> (SELECT TOP 1 APP_SID FROM DW_ODIN.APP_DIM WHERE APP_NAME = 'cricket' AND APP_ID = '1k')`});
        res = etlLoad2.execute();        
        res.next();
        row_updated = res.getColumnValue(1);
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = ` 
                      + row_updated
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
    $$
    ;
--Need to modify for USER_SID change. Updated for performance tuning
CREATE OR REPLACE PROCEDURE "LOAD_HOURLY_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
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
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to merge into HOURLY_TVS_AGG for KPI calculations' FROM DUAL"});                                  
    
     try {

        beginTransaction.execute();
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE DW_ODIN.HOURLY_TVS_AGG_CMS 
                      AS  SELECT HOURLY_TVS_AGG.HOUR_SID, 
                              HOURLY_TVS_AGG.CLIENT_SID, 
                              HOURLY_TVS_AGG.SESSION_ID, 
                              HOURLY_TVS_AGG.CMS_CHANNEL_SID OLD_CMS_CHANNEL_SID,
                              HOURLY_TVS_AGG.CMS_EPISODE_SID OLD_CMS_EPISODE_SID, 
                              HOURLY_TVS_AGG.CMS_CLIP_SID OLD_CMS_CLIP_SID, 
                              HOURLY_TVS_AGG.CMS_TIMELINE_SID OLD_CMS_TIMELINE_SID,
                              HOURLY_TVS_AGG.CMS_USER_SID OLD_CMS_USER_SID,
                              HOURLY_TVS_AGG.APP_SID,
                              HOURLY_TVS_AGG.APP_VERSION,
                              HOURLY_TVS_AGG.GEO_SID,
                              HOURLY_TVS_AGG.UTM_SID,
                              HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE,
                              HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC,
                              --get the current channel_id and current channel_sid that is in the table
                              --the goal is to join via channel_id, and get the new channel_sid, if it's changed, later
                              CMS_CHANNEL_DIM.CHANNEL_ID CMS_CHANNEL_ID, 
                              CMS_EPISODE_DIM.EPISODE_ID CMS_EPISODE_ID, 
                              CMS_CLIP_DIM.CLIP_ID  CMS_CLIP_ID, 
                              CMS_TIMELINE_DIM.TIMELINE_ID CMS_TIMELINE_ID,
                              CMS_USER_DIM.USER_ID CMS_USER_ID
                            FROM RPT.HOURLY_TVS_AGG
                            JOIN DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED ON HOURLY_TVS_AGG.SESSION_ID = CLIENT_SESSION_TO_BE_UPDATED.SESSION_ID
                            AND HOURLY_TVS_AGG.CLIENT_SID = CLIENT_SESSION_TO_BE_UPDATED.CLIENT_SID   
                            JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID     
                            JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                            JOIN DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                            JOIN DW_ODIN.CMS_TIMELINE_DIM ON HOURLY_TVS_AGG.CMS_TIMELINE_SID = CMS_TIMELINE_DIM.CMS_TIMELINE_SID
                            JOIN DW_ODIN.CMS_USER_DIM_VW CMS_USER_DIM ON HOURLY_TVS_AGG.CMS_USER_SID = CMS_USER_DIM.CMS_USER_SID  
                            `});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE RPT.second_dataset as (select CLIENT_VIDEO_SEGMENTS.CLIENT_SID
                          															,CLIENT_VIDEO_SEGMENTS.SESSION_ID
                          															,CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_SID
                          															,CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_ID
                                            										,CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_ID
                                            										,CLIENT_VIDEO_SEGMENTS.CMS_CLIP_ID
                                           											,CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_ID
                                           											,CLIENT_VIDEO_SEGMENTS.CMS_USER_ID
                          															,CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_SID
																					,CLIENT_VIDEO_SEGMENTS.CMS_CLIP_SID
       																				,CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_SID
                                                   									,CLIENT_VIDEO_SEGMENTS.CMS_USER_SID
                                                   									,CLIENT_VIDEO_SEGMENTS.APP_SID
                          															,CLIENT_VIDEO_SEGMENTS.APP_VERSION
                          															,CLIENT_VIDEO_SEGMENTS.GEO_SID
                          															,CLIENT_VIDEO_SEGMENTS.UTM_SID
                          															,CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_TYPE
                          															,CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_BEGIN_UTC
                          															,CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC          
                                  										from         DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED 
                          												JOIN 		DW_ODIN.CLIENT_VIDEO_SEGMENTS CLIENT_VIDEO_SEGMENTS
                          												ON 			CLIENT_VIDEO_SEGMENTS.SESSION_ID = CLIENT_SESSION_TO_BE_UPDATED.SESSION_ID
                              											AND 		CLIENT_VIDEO_SEGMENTS.CLIENT_SID = CLIENT_SESSION_TO_BE_UPDATED.CLIENT_SID    
                          												JOIN 		DW_ODIN.ACTIVE_SESSION
                          												ON 			CLIENT_VIDEO_SEGMENTS.APP_SID = ACTIVE_SESSION.APP_SID
                          												AND 		CLIENT_VIDEO_SEGMENTS.SESSION_ID = ACTIVE_SESSION.SESSION_ID
                          												AND 		CLIENT_VIDEO_SEGMENTS.CLIENT_SID = ACTIVE_SESSION.CLIENT_SID)`});
        etlLoad2.execute(); 
		
		
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE DW_ODIN.HOURLY_TVS_AGG_TO_BE_UPDATED
                      AS  
                      WITH LHOUR AS (select UTC, HOUR_SID from DW_ODIN.HOUR_DIM where UTC between '2019-01-01' and current_timestamp()) 
                        select
                          HOUR.HOUR_SID
                          ,second_dataset.CLIENT_SID
                          ,second_dataset.SESSION_ID
                          ,second_dataset.CMS_CHANNEL_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_CHANNEL_SID
                          ,second_dataset.CMS_EPISODE_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_EPISODE_SID
                          ,second_dataset.CMS_CLIP_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_CLIP_SID
                          ,second_dataset.CMS_TIMELINE_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_TIMELINE_SID
                          ,second_dataset.CMS_USER_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_USER_SID
                          ,second_dataset.APP_SID
                          ,second_dataset.APP_VERSION
                          ,second_dataset.GEO_SID
                          ,second_dataset.UTM_SID
                          ,second_dataset.VIDEO_SEGMENT_TYPE
                          ,second_dataset.VIDEO_SEGMENT_BEGIN_UTC
                          ,sum(case when DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_BEGIN_UTC) = HOUR.UTC 
                                  and DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_END_UTC) = HOUR.UTC
                                  then datediff('seconds', second_dataset.VIDEO_SEGMENT_BEGIN_UTC, second_dataset.VIDEO_SEGMENT_END_UTC)
                              when DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_BEGIN_UTC) < HOUR.UTC  
                                  and DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_END_UTC) = HOUR.UTC
                                  then datediff('seconds', HOUR.UTC, second_dataset.VIDEO_SEGMENT_END_UTC)
                              when DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_BEGIN_UTC) = HOUR.UTC
                                  and DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_END_UTC) > HOUR.UTC
                                  then datediff('seconds', second_dataset.VIDEO_SEGMENT_BEGIN_UTC, DATEADD('hour',1,HOUR.UTC))
                              when DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_BEGIN_UTC) < HOUR.UTC
                                  and DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_END_UTC) > HOUR.UTC
                                  then 3600 
                              end) as TOTAL_VIEWING_SECONDS
                          from RPT.second_dataset
                          join LHOUR as HOUR
                          on HOUR.UTC between DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_BEGIN_UTC) and DATE_TRUNC('hour', second_dataset.VIDEO_SEGMENT_END_UTC)
                          --left join to the existing s_hourly_agg table via channel_id (immutable), with its channel_id and old cms_channel_sid
                          LEFT JOIN DW_ODIN.HOURLY_TVS_AGG_CMS
                           ON HOUR.HOUR_SID = HOURLY_TVS_AGG_CMS.HOUR_SID
                            AND second_dataset.CLIENT_SID = HOURLY_TVS_AGG_CMS.CLIENT_SID
                            AND IFNULL(second_dataset.SESSION_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.SESSION_ID, 'NA')
                            AND IFNULL(second_dataset.CMS_CHANNEL_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_CHANNEL_ID, 'NA')
                            AND IFNULL(second_dataset.CMS_EPISODE_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_EPISODE_ID, 'NA')
                            AND IFNULL(second_dataset.CMS_CLIP_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_CLIP_ID, 'NA')
                            AND IFNULL(second_dataset.CMS_TIMELINE_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_TIMELINE_ID, 'NA')
                            AND IFNULL(second_dataset.CMS_USER_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_USER_ID, 'NA')
                            AND second_dataset.APP_SID = HOURLY_TVS_AGG_CMS.APP_SID
                            AND IFNULL(second_dataset.APP_VERSION, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.APP_VERSION, 'NA')
                            AND second_dataset.GEO_SID = HOURLY_TVS_AGG_CMS.GEO_SID
                            AND second_dataset.UTM_SID = HOURLY_TVS_AGG_CMS.UTM_SID
                            AND IFNULL(second_dataset.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.VIDEO_SEGMENT_TYPE, 'NA')
                            AND second_dataset.VIDEO_SEGMENT_BEGIN_UTC = HOURLY_TVS_AGG_CMS.VIDEO_SEGMENT_BEGIN_UTC 
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19`});
						  
        etlLoad3.execute(); 
	   
        var etlLoad4 = snowflake.createStatement(
          {sqlText:`MERGE INTO RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                      USING DW_ODIN.HOURLY_TVS_AGG_TO_BE_UPDATED STG_HOURLY_TVS_AGG
                      ON HOURLY_TVS_AGG.HOUR_SID = STG_HOURLY_TVS_AGG.HOUR_SID
                      AND HOURLY_TVS_AGG.CLIENT_SID = STG_HOURLY_TVS_AGG.CLIENT_SID
                      AND IFNULL(HOURLY_TVS_AGG.SESSION_ID, 'NA') = IFNULL(STG_HOURLY_TVS_AGG.SESSION_ID, 'NA')
                      --if old_cms_channel_sid has a value, that means it's NOT a new record
                      --if old_cms_channel_sid is null, that means it's  a new record and should be inserted
                      AND CAST(HOURLY_TVS_AGG.CMS_CHANNEL_SID AS VARCHAR) = IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_CHANNEL_SID AS VARCHAR), 'New Record')
                      AND CAST(HOURLY_TVS_AGG.CMS_EPISODE_SID AS VARCHAR) =  IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_EPISODE_SID AS VARCHAR), 'New Record')
                      AND CAST(HOURLY_TVS_AGG.CMS_CLIP_SID AS VARCHAR) =  IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_CLIP_SID AS VARCHAR), 'New Record')
                      AND CAST(HOURLY_TVS_AGG.CMS_TIMELINE_SID AS VARCHAR) =  IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_TIMELINE_SID AS VARCHAR), 'New Record')
                      AND CAST(HOURLY_TVS_AGG.CMS_USER_SID AS VARCHAR) = IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_USER_SID AS VARCHAR), 'New Record')
                      AND HOURLY_TVS_AGG.APP_SID = STG_HOURLY_TVS_AGG.APP_SID
                      AND IFNULL(HOURLY_TVS_AGG.APP_VERSION, 'NA') = IFNULL(STG_HOURLY_TVS_AGG.APP_VERSION, 'NA')
                      AND HOURLY_TVS_AGG.GEO_SID = STG_HOURLY_TVS_AGG.GEO_SID
                      AND HOURLY_TVS_AGG.UTM_SID = STG_HOURLY_TVS_AGG.UTM_SID
                      AND IFNULL(HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(STG_HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA')
                      AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = STG_HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC
                      WHEN MATCHED 
                      AND (HOURLY_TVS_AGG.CMS_CHANNEL_SID <> STG_HOURLY_TVS_AGG.CMS_CHANNEL_SID
                           OR HOURLY_TVS_AGG.CMS_EPISODE_SID <> STG_HOURLY_TVS_AGG.CMS_EPISODE_SID
                           OR HOURLY_TVS_AGG.CMS_CLIP_SID <> STG_HOURLY_TVS_AGG.CMS_CLIP_SID
                           OR HOURLY_TVS_AGG.CMS_TIMELINE_SID <> STG_HOURLY_TVS_AGG.CMS_TIMELINE_SID
                           OR HOURLY_TVS_AGG.CMS_USER_SID <> STG_HOURLY_TVS_AGG.CMS_USER_SID
                           or HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS <> STG_HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS)
                      THEN UPDATE SET HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS = STG_HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS,
                           HOURLY_TVS_AGG.CMS_CHANNEL_SID = STG_HOURLY_TVS_AGG.CMS_CHANNEL_SID,
                           HOURLY_TVS_AGG.CMS_EPISODE_SID = STG_HOURLY_TVS_AGG.CMS_EPISODE_SID,
                           HOURLY_TVS_AGG.CMS_CLIP_SID = STG_HOURLY_TVS_AGG.CMS_CLIP_SID,
                           HOURLY_TVS_AGG.CMS_TIMELINE_SID = STG_HOURLY_TVS_AGG.CMS_TIMELINE_SID,
                           HOURLY_TVS_AGG.CMS_USER_SID = STG_HOURLY_TVS_AGG.CMS_USER_SID,
                           HOURLY_TVS_AGG.ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                      WHEN NOT MATCHED THEN INSERT (HOUR_SID, CLIENT_SID, SESSION_ID, CMS_CHANNEL_SID, CMS_EPISODE_SID, CMS_CLIP_SID, CMS_TIMELINE_SID, CMS_USER_SID, APP_SID, APP_VERSION, GEO_SID, 
                                                    UTM_SID, VIDEO_SEGMENT_TYPE, VIDEO_SEGMENT_BEGIN_UTC, TOTAL_VIEWING_SECONDS, ETL_LOAD_UTC)
                      VALUES (HOUR_SID, CLIENT_SID, SESSION_ID, CMS_CHANNEL_SID, CMS_EPISODE_SID, CMS_CLIP_SID, CMS_TIMELINE_SID, CMS_USER_SID, APP_SID, APP_VERSION, GEO_SID, 
                                                    UTM_SID, VIDEO_SEGMENT_TYPE, VIDEO_SEGMENT_BEGIN_UTC, TOTAL_VIEWING_SECONDS, current_timestamp::timestamp_ntz)`});
        res = etlLoad4.execute();        
        res.next();
        row_inserted = res.getColumnValue(1);
        row_updated = res.getColumnValue(2);                
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, RECORDS_UPDATED = `
                      + row_updated
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
$$    
;

--Need to modify for USER_SID   MODIFIED as part of performance tuning
CREATE OR REPLACE PROCEDURE "LOAD_HOURLY_INACTIVE_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$    
        var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to merge into HOURLY_INACTIVE_TVS_AGG for KPI calculations' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE DW_ODIN.HOURLY_INACTIVE_TVS_AGG_CMS 
                    AS  SELECT HOURLY_TVS_AGG.HOUR_SID, 
                            HOURLY_TVS_AGG.CLIENT_SID, 
                            HOURLY_TVS_AGG.SESSION_ID, 
                            HOURLY_TVS_AGG.CMS_CHANNEL_SID OLD_CMS_CHANNEL_SID,
                            HOURLY_TVS_AGG.CMS_EPISODE_SID OLD_CMS_EPISODE_SID, 
                            HOURLY_TVS_AGG.CMS_CLIP_SID OLD_CMS_CLIP_SID, 
                            HOURLY_TVS_AGG.CMS_TIMELINE_SID OLD_CMS_TIMELINE_SID,
                            HOURLY_TVS_AGG.CMS_USER_SID OLD_CMS_USER_SID,
                            HOURLY_TVS_AGG.APP_SID,
                            HOURLY_TVS_AGG.APP_VERSION,
                            HOURLY_TVS_AGG.GEO_SID,
                            HOURLY_TVS_AGG.UTM_SID,
                            HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE,
                            HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC,
                            --get the current channel_id and current channel_sid that is in the table
                            --the goal is to join via channel_id, and get the new channel_sid, if it's changed, later
                            CMS_CHANNEL_DIM.CHANNEL_ID CMS_CHANNEL_ID, 
                            CMS_EPISODE_DIM.EPISODE_ID CMS_EPISODE_ID, 
                            CMS_CLIP_DIM.CLIP_ID  CMS_CLIP_ID, 
                            CMS_TIMELINE_DIM.TIMELINE_ID CMS_TIMELINE_ID,
                            CMS_USER_DIM.USER_ID CMS_USER_ID
                          FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                          JOIN DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED ON HOURLY_TVS_AGG.SESSION_ID = CLIENT_SESSION_TO_BE_UPDATED.SESSION_ID
                          AND HOURLY_TVS_AGG.CLIENT_SID = CLIENT_SESSION_TO_BE_UPDATED.CLIENT_SID   
                          JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID     
                          JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                          JOIN DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                          JOIN DW_ODIN.CMS_TIMELINE_DIM ON HOURLY_TVS_AGG.CMS_TIMELINE_SID = CMS_TIMELINE_DIM.CMS_TIMELINE_SID
                          JOIN DW_ODIN.CMS_USER_DIM_VW CMS_USER_DIM ON HOURLY_TVS_AGG.CMS_USER_SID = CMS_USER_DIM.CMS_USER_SID    
                          `});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE RPT.INACTIVE_AGG_DATA_TO_UPDATE AS
                 select     CLIENT_VIDEO_SEGMENTS.CLIENT_SID
                           ,CLIENT_VIDEO_SEGMENTS.SESSION_ID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_SID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_SID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_CLIP_SID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_SID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_USER_SID
                           ,CLIENT_VIDEO_SEGMENTS.APP_SID
                           ,CLIENT_VIDEO_SEGMENTS.APP_VERSION
                           ,CLIENT_VIDEO_SEGMENTS.GEO_SID
                           ,CLIENT_VIDEO_SEGMENTS.UTM_SID
                           ,CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_TYPE
                           ,CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_BEGIN_UTC
                           ,CLIENT_VIDEO_SEGMENTS.VIDEO_SEGMENT_END_UTC
                           ,CLIENT_VIDEO_SEGMENTS.CMS_CHANNEL_ID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_EPISODE_ID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_CLIP_ID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_TIMELINE_ID
                           ,CLIENT_VIDEO_SEGMENTS.CMS_USER_ID
                  from     DW_ODIN.CLIENT_VIDEO_SEGMENTS CLIENT_VIDEO_SEGMENTS
                  JOIN     DW_ODIN.CLIENT_SESSION_TO_BE_UPDATED ON CLIENT_VIDEO_SEGMENTS.SESSION_ID = CLIENT_SESSION_TO_BE_UPDATED.SESSION_ID
                  AND      CLIENT_VIDEO_SEGMENTS.CLIENT_SID = CLIENT_SESSION_TO_BE_UPDATED.CLIENT_SID`});
        etlLoad2.execute(); 
		
		  var etlLoad3 = snowflake.createStatement(
          {sqlText:` CREATE OR REPLACE TEMPORARY TABLE DW_ODIN.HOURLY_INACTIVE_TVS_AGG_TO_BE_UPDATED
                      AS WITH HOUR_DIMENSION AS (select UTC, HOUR_SID from DW_ODIN.HOUR_DIM where UTC between '2019-01-01' and current_timestamp())   
                         select
                          HOUR.HOUR_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.CLIENT_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.SESSION_ID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.CMS_CHANNEL_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_CHANNEL_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.CMS_EPISODE_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_EPISODE_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.CMS_CLIP_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_CLIP_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.CMS_TIMELINE_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_TIMELINE_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.CMS_USER_SID
                          ,HOURLY_TVS_AGG_CMS.OLD_CMS_USER_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.APP_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.APP_VERSION
                          ,INACTIVE_AGG_DATA_TO_UPDATE.GEO_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.UTM_SID
                          ,INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_TYPE
                          ,INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC
                          ,sum(case when DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC) = HOUR.UTC 
                                  and DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_END_UTC) = HOUR.UTC
                                  then datediff('seconds', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC, INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_END_UTC)
                              when DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC) < HOUR.UTC  
                                  and DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_END_UTC) = HOUR.UTC
                                  then datediff('seconds', HOUR.UTC, INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_END_UTC)
                              when DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC) = HOUR.UTC
                                  and DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_END_UTC) > HOUR.UTC
                                  then datediff('seconds', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC, DATEADD('hour',1,HOUR.UTC))
                              when DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC) < HOUR.UTC
                                  and DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_END_UTC) > HOUR.UTC
                                  then 3600 
                              end) as TOTAL_VIEWING_SECONDS
                          from RPT.INACTIVE_AGG_DATA_TO_UPDATE
                          join HOUR_DIMENSION as HOUR
                          on HOUR.UTC between DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC) and DATE_TRUNC('hour', INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_END_UTC)
                          --left join to the existing s_hourly_agg table via channel_id (immutable), with its channel_id and old cms_channel_sid
                          LEFT JOIN DW_ODIN.HOURLY_INACTIVE_TVS_AGG_CMS HOURLY_TVS_AGG_CMS
                           ON HOUR.HOUR_SID = HOURLY_TVS_AGG_CMS.HOUR_SID
                            AND INACTIVE_AGG_DATA_TO_UPDATE.CLIENT_SID = HOURLY_TVS_AGG_CMS.CLIENT_SID
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.SESSION_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.SESSION_ID, 'NA')
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.CMS_CHANNEL_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_CHANNEL_ID, 'NA')
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.CMS_EPISODE_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_EPISODE_ID, 'NA')
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.CMS_CLIP_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_CLIP_ID, 'NA')
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.CMS_TIMELINE_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_TIMELINE_ID, 'NA')
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.CMS_USER_ID, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.CMS_USER_ID, 'NA')
                            AND INACTIVE_AGG_DATA_TO_UPDATE.APP_SID = HOURLY_TVS_AGG_CMS.APP_SID
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.APP_VERSION, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.APP_VERSION, 'NA')
                            AND INACTIVE_AGG_DATA_TO_UPDATE.GEO_SID = HOURLY_TVS_AGG_CMS.GEO_SID
                            AND INACTIVE_AGG_DATA_TO_UPDATE.UTM_SID = HOURLY_TVS_AGG_CMS.UTM_SID
                            AND IFNULL(INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(HOURLY_TVS_AGG_CMS.VIDEO_SEGMENT_TYPE, 'NA')
                            AND INACTIVE_AGG_DATA_TO_UPDATE.VIDEO_SEGMENT_BEGIN_UTC = HOURLY_TVS_AGG_CMS.VIDEO_SEGMENT_BEGIN_UTC 
                          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19`});
        etlLoad3.execute(); 
        
        var etlLoad4 = snowflake.createStatement(
          {sqlText:`MERGE INTO RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    USING DW_ODIN.HOURLY_INACTIVE_TVS_AGG_TO_BE_UPDATED STG_HOURLY_TVS_AGG
                    ON HOURLY_TVS_AGG.HOUR_SID = STG_HOURLY_TVS_AGG.HOUR_SID
                    AND HOURLY_TVS_AGG.CLIENT_SID = STG_HOURLY_TVS_AGG.CLIENT_SID
                    AND IFNULL(HOURLY_TVS_AGG.SESSION_ID, 'NA') = IFNULL(STG_HOURLY_TVS_AGG.SESSION_ID, 'NA')
                    --if old_cms_channel_sid has a value, that means it's NOT a new record
                    --if old_cms_channel_sid is null, that means it's  a new record and should be inserted
                    AND CAST(HOURLY_TVS_AGG.CMS_CHANNEL_SID AS VARCHAR) = IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_CHANNEL_SID AS VARCHAR), 'New Record')
                    AND CAST(HOURLY_TVS_AGG.CMS_EPISODE_SID AS VARCHAR) =  IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_EPISODE_SID AS VARCHAR), 'New Record')
                    AND CAST(HOURLY_TVS_AGG.CMS_CLIP_SID AS VARCHAR) =  IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_CLIP_SID AS VARCHAR), 'New Record')
                    AND CAST(HOURLY_TVS_AGG.CMS_TIMELINE_SID AS VARCHAR) =  IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_TIMELINE_SID AS VARCHAR), 'New Record')
                    AND CAST(HOURLY_TVS_AGG.CMS_USER_SID AS VARCHAR) = IFNULL(CAST(STG_HOURLY_TVS_AGG.OLD_CMS_USER_SID AS VARCHAR), 'New Record')
                    AND HOURLY_TVS_AGG.APP_SID = STG_HOURLY_TVS_AGG.APP_SID
                    AND IFNULL(HOURLY_TVS_AGG.APP_VERSION, 'NA') = IFNULL(STG_HOURLY_TVS_AGG.APP_VERSION, 'NA')
                    AND HOURLY_TVS_AGG.GEO_SID = STG_HOURLY_TVS_AGG.GEO_SID
                    AND HOURLY_TVS_AGG.UTM_SID = STG_HOURLY_TVS_AGG.UTM_SID
                    AND IFNULL(HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(STG_HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA')
                    AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = STG_HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC
                    WHEN MATCHED 
                    AND (HOURLY_TVS_AGG.CMS_CHANNEL_SID <> STG_HOURLY_TVS_AGG.CMS_CHANNEL_SID
                         OR HOURLY_TVS_AGG.CMS_EPISODE_SID <> STG_HOURLY_TVS_AGG.CMS_EPISODE_SID
                         OR HOURLY_TVS_AGG.CMS_CLIP_SID <> STG_HOURLY_TVS_AGG.CMS_CLIP_SID
                         OR HOURLY_TVS_AGG.CMS_TIMELINE_SID <> STG_HOURLY_TVS_AGG.CMS_TIMELINE_SID
                         OR HOURLY_TVS_AGG.CMS_USER_SID <> STG_HOURLY_TVS_AGG.CMS_USER_SID
                         or HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS <> STG_HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS)
                    THEN UPDATE SET HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS = STG_HOURLY_TVS_AGG.TOTAL_VIEWING_SECONDS,
                         HOURLY_TVS_AGG.CMS_CHANNEL_SID = STG_HOURLY_TVS_AGG.CMS_CHANNEL_SID,
                         HOURLY_TVS_AGG.CMS_EPISODE_SID = STG_HOURLY_TVS_AGG.CMS_EPISODE_SID,
                         HOURLY_TVS_AGG.CMS_CLIP_SID = STG_HOURLY_TVS_AGG.CMS_CLIP_SID,
                         HOURLY_TVS_AGG.CMS_TIMELINE_SID = STG_HOURLY_TVS_AGG.CMS_TIMELINE_SID,
                         HOURLY_TVS_AGG.CMS_USER_SID = STG_HOURLY_TVS_AGG.CMS_USER_SID,
                         HOURLY_TVS_AGG.ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                    WHEN NOT MATCHED THEN INSERT (HOUR_SID, CLIENT_SID, SESSION_ID, CMS_CHANNEL_SID, CMS_EPISODE_SID, CMS_CLIP_SID, CMS_TIMELINE_SID, CMS_USER_SID, APP_SID, APP_VERSION, GEO_SID, 
                                                  UTM_SID, VIDEO_SEGMENT_TYPE, VIDEO_SEGMENT_BEGIN_UTC, TOTAL_VIEWING_SECONDS, ETL_LOAD_UTC)
                    VALUES (HOUR_SID, CLIENT_SID, SESSION_ID, CMS_CHANNEL_SID, CMS_EPISODE_SID, CMS_CLIP_SID, CMS_TIMELINE_SID, CMS_USER_SID, APP_SID, APP_VERSION, GEO_SID, 
                                                  UTM_SID, VIDEO_SEGMENT_TYPE, VIDEO_SEGMENT_BEGIN_UTC, TOTAL_VIEWING_SECONDS, current_timestamp::timestamp_ntz)`});
        res = etlLoad4.execute();        
        res.next();
        row_inserted = res.getColumnValue(1);
        row_updated = res.getColumnValue(2);                
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_INSERTED = ` 
                      + row_inserted 
                      + `, RECORDS_UPDATED = `
                      + row_updated
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
$$    
;

CREATE OR REPLACE PROCEDURE "UPDATE_TIMELINE_FLAG_HOURLY_INACTIVE_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_INACTIVE_TVS_AGG with timeline misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE INACTIVE_TIMELINE_ALIGNED AS (
                        WITH VOD_COLLECTION AS (SELECT EPISODE_ID FROM DW_ODIN.CMS_VODCATEGORYENTRIES_DIM
                          UNION 
                          SELECT EPISODE_ID
                          FROM DW_ODIN.CMS_EPISODE_DIM
                          WHERE SERIES_ID IN (SELECT SERIES_ID FROM DW_ODIN.CMS_VODCATEGORYENTRIES_DIM))   

                      SELECT HOURLY_TVS_AGG.*
                      FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                      JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                      JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                      WHERE  CMS_CHANNEL_DIM.CHANNEL_ID <> 'vod' 
                        AND (HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG = FALSE)
                        AND EXISTS (
                          SELECT 1 FROM STG.CMS_MONGO_TIMELINES_OVERWRITE_VW CMS_MONGO_TIMELINES_OVERWRITE
                          WHERE CMS_CHANNEL_DIM.CHANNEL_ID = CMS_MONGO_TIMELINES_OVERWRITE.CHANNEL_ID
                          AND CMS_EPISODE_DIM.EPISODE_ID = CMS_MONGO_TIMELINES_OVERWRITE.EPISODE_ID
                          AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC BETWEEN CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_START_UTC_WITH_BUFFER AND CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_STOP_UTC) 

                      UNION ALL

                      SELECT HOURLY_TVS_AGG.*
                      FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                      JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                      JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                      JOIN VOD_COLLECTION ON VOD_COLLECTION.EPISODE_ID = CMS_EPISODE_DIM.EPISODE_ID
                      WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' 
                        AND (HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG = FALSE)
                        AND EXISTS (SELECT 1
                                     FROM DW_ODIN.CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW
                                     WHERE CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                                     AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC 
                                      BETWEEN CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_START_UTC AND CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_END_UTC
                                     AND CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.DISTRIBUTION_TYPE = 'AVOD')  
                         )`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                        SET TIMELINE_ALIGNED_FLAG = TRUE, 
                            ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                        FROM INACTIVE_TIMELINE_ALIGNED TIMELINE_ALIGNED
                        WHERE HOURLY_TVS_AGG.HOUR_SID = TIMELINE_ALIGNED.HOUR_SID
                        AND HOURLY_TVS_AGG.CLIENT_SID = TIMELINE_ALIGNED.CLIENT_SID 
                        AND HOURLY_TVS_AGG.SESSION_ID = TIMELINE_ALIGNED.SESSION_ID 
                        AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = TIMELINE_ALIGNED.CMS_CHANNEL_SID 
                        AND HOURLY_TVS_AGG.CMS_EPISODE_SID = TIMELINE_ALIGNED.CMS_EPISODE_SID 
                        AND HOURLY_TVS_AGG.CMS_CLIP_SID = TIMELINE_ALIGNED.CMS_CLIP_SID 
                        AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = TIMELINE_ALIGNED.CMS_TIMELINE_SID 
                        AND HOURLY_TVS_AGG.APP_SID = TIMELINE_ALIGNED.APP_SID 
                        AND HOURLY_TVS_AGG.APP_VERSION = TIMELINE_ALIGNED.APP_VERSION 
                        AND HOURLY_TVS_AGG.GEO_SID = TIMELINE_ALIGNED.GEO_SID 
                        AND HOURLY_TVS_AGG.UTM_SID = TIMELINE_ALIGNED.UTM_SID 
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE = TIMELINE_ALIGNED.VIDEO_SEGMENT_TYPE 
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = TIMELINE_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                      SET TIMELINE_ALIGNED_FLAG = FALSE,
                        ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                      WHERE TIMELINE_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
$$
;

CREATE OR REPLACE PROCEDURE "UPDATE_TIMELINE_FLAG_HOURLY_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$

    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_TVS_AGG with timeline misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE TIMELINE_ALIGNED AS (
                      WITH VOD_COLLECTION AS (SELECT EPISODE_ID FROM DW_ODIN.CMS_VODCATEGORYENTRIES_DIM
                        UNION 
                        SELECT EPISODE_ID
                        FROM DW_ODIN.CMS_EPISODE_DIM
                        WHERE SERIES_ID IN (SELECT SERIES_ID FROM DW_ODIN.CMS_VODCATEGORYENTRIES_DIM))   

                    SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                    JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    WHERE  CMS_CHANNEL_DIM.CHANNEL_ID <> 'vod' AND (HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG = FALSE)
                      AND EXISTS (
                        SELECT 1 FROM STG.CMS_MONGO_TIMELINES_OVERWRITE_VW CMS_MONGO_TIMELINES_OVERWRITE
                        WHERE CMS_CHANNEL_DIM.CHANNEL_ID = CMS_MONGO_TIMELINES_OVERWRITE.CHANNEL_ID
                        AND CMS_EPISODE_DIM.EPISODE_ID = CMS_MONGO_TIMELINES_OVERWRITE.EPISODE_ID
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC BETWEEN CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_START_UTC_WITH_BUFFER AND CMS_MONGO_TIMELINES_OVERWRITE.TIMELINE_STOP_UTC) 

                    UNION ALL

                    SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                    JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                    JOIN VOD_COLLECTION ON VOD_COLLECTION.EPISODE_ID = CMS_EPISODE_DIM.EPISODE_ID
                    WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' AND (HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.TIMELINE_ALIGNED_FLAG = FALSE)
                      AND EXISTS (SELECT 1
                                   FROM DW_ODIN.CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW
                                   WHERE CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                                   AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC 
                                    BETWEEN CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_START_UTC AND CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.EPISODE_AVAIL_END_UTC
                                   AND CMS_EPISODE_AVAIL_WINDOWS_PARSED_VW.DISTRIBUTION_TYPE = 'AVOD')  
                        );`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                      SET TIMELINE_ALIGNED_FLAG = TRUE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                      FROM TIMELINE_ALIGNED TIMELINE_ALIGNED
                      WHERE HOURLY_TVS_AGG.HOUR_SID = TIMELINE_ALIGNED.HOUR_SID
                      AND HOURLY_TVS_AGG.CLIENT_SID = TIMELINE_ALIGNED.CLIENT_SID 
                      AND HOURLY_TVS_AGG.SESSION_ID = TIMELINE_ALIGNED.SESSION_ID 
                      AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = TIMELINE_ALIGNED.CMS_CHANNEL_SID 
                      AND HOURLY_TVS_AGG.CMS_EPISODE_SID = TIMELINE_ALIGNED.CMS_EPISODE_SID 
                      AND HOURLY_TVS_AGG.CMS_CLIP_SID = TIMELINE_ALIGNED.CMS_CLIP_SID 
                      AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = TIMELINE_ALIGNED.CMS_TIMELINE_SID 
                      AND HOURLY_TVS_AGG.APP_SID = TIMELINE_ALIGNED.APP_SID 
                      AND IFNULL(HOURLY_TVS_AGG.APP_VERSION, 'NA') = IFNULL(TIMELINE_ALIGNED.APP_VERSION, 'NA')
                      AND HOURLY_TVS_AGG.GEO_SID = TIMELINE_ALIGNED.GEO_SID 
                      AND HOURLY_TVS_AGG.UTM_SID = TIMELINE_ALIGNED.UTM_SID 
                      AND IFNULL(HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(TIMELINE_ALIGNED.VIDEO_SEGMENT_TYPE , 'NA')
                      AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = TIMELINE_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                      SET TIMELINE_ALIGNED_FLAG = FALSE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                      WHERE TIMELINE_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
$$    
;
    
CREATE OR REPLACE PROCEDURE "UPDATE_CLIP_WINDOW_ALIGNED_FLAG_HOURLY_INACTIVE_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_INACTIVE_TVS_AGG with clip window misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE INACTIVE_CLIP_WINDOW_ALIGNED AS
                    (SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    WHERE  IFNULL(CMS_CHANNEL_DIM.CHANNEL_ID, 'NA') <> 'vod' AND (HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG = FALSE)
                      AND EXISTS (
                                SELECT 1 FROM DW_ODIN.CMS_CLIP_AVAIL_WINDOWS_PARSED_VW
                                WHERE HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CMS_CLIP_SID
                                AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC
                                AND UPPER(CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.distribution_type) = 'LINEAR'
                                )

                    UNION ALL

                    SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                    WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' AND (HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG = FALSE)
                      AND EXISTS (
                                SELECT 1 FROM DW_ODIN.CMS_CLIP_AVAIL_WINDOWS_PARSED_VW
                                WHERE HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CMS_CLIP_SID
                                AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC
                                AND UPPER(CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.distribution_type) = 'AVOD'
                                )
                       );`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`   UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                        SET CLIP_WINDOW_ALIGNED_FLAG = TRUE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                        FROM INACTIVE_CLIP_WINDOW_ALIGNED CLIP_WINDOW_ALIGNED
                        WHERE HOURLY_TVS_AGG.HOUR_SID = CLIP_WINDOW_ALIGNED.HOUR_SID
                        AND HOURLY_TVS_AGG.CLIENT_SID = CLIP_WINDOW_ALIGNED.CLIENT_SID 
                        AND HOURLY_TVS_AGG.SESSION_ID = CLIP_WINDOW_ALIGNED.SESSION_ID 
                        AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = CLIP_WINDOW_ALIGNED.CMS_CHANNEL_SID 
                        AND HOURLY_TVS_AGG.CMS_EPISODE_SID = CLIP_WINDOW_ALIGNED.CMS_EPISODE_SID 
                        AND HOURLY_TVS_AGG.CMS_CLIP_SID = CLIP_WINDOW_ALIGNED.CMS_CLIP_SID 
                        AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = CLIP_WINDOW_ALIGNED.CMS_TIMELINE_SID 
                        AND HOURLY_TVS_AGG.APP_SID = CLIP_WINDOW_ALIGNED.APP_SID 
                        AND HOURLY_TVS_AGG.APP_VERSION = CLIP_WINDOW_ALIGNED.APP_VERSION 
                        AND HOURLY_TVS_AGG.GEO_SID = CLIP_WINDOW_ALIGNED.GEO_SID 
                        AND HOURLY_TVS_AGG.UTM_SID = CLIP_WINDOW_ALIGNED.UTM_SID 
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE = CLIP_WINDOW_ALIGNED.VIDEO_SEGMENT_TYPE 
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = CLIP_WINDOW_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC ;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    SET CLIP_WINDOW_ALIGNED_FLAG = FALSE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                    WHERE CLIP_WINDOW_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
$$    
;


CREATE OR REPLACE PROCEDURE "UPDATE_CLIP_WINDOW_ALIGNED_FLAG_HOURLY_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_TVS_AGG with clip window misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE CLIP_WINDOW_ALIGNED AS
                      (SELECT HOURLY_TVS_AGG.*
                      FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                      JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                      WHERE  IFNULL(CMS_CHANNEL_DIM.CHANNEL_ID, 'NA') <> 'vod' AND (HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG = FALSE)
                        AND EXISTS (
                                  SELECT 1 FROM DW_ODIN.CMS_CLIP_AVAIL_WINDOWS_PARSED_VW
                                  WHERE HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CMS_CLIP_SID
                                  AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC
                                  AND UPPER(CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.distribution_type) = 'LINEAR'
                                  )

                      UNION ALL

                      SELECT HOURLY_TVS_AGG.*
                      FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                      JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                      WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' AND (HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG IS NULL OR HOURLY_TVS_AGG.CLIP_WINDOW_ALIGNED_FLAG = FALSE)
                        AND EXISTS (
                                  SELECT 1 FROM DW_ODIN.CMS_CLIP_AVAIL_WINDOWS_PARSED_VW
                                  WHERE HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CMS_CLIP_SID
                                  AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC BETWEEN CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_START_UTC AND CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.CLIP_AVAIL_END_UTC
                                  AND UPPER(CMS_CLIP_AVAIL_WINDOWS_PARSED_VW.distribution_type) = 'AVOD'
                                  )
                         );`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`  UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                      SET CLIP_WINDOW_ALIGNED_FLAG = TRUE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                      FROM CLIP_WINDOW_ALIGNED CLIP_WINDOW_ALIGNED
                      WHERE HOURLY_TVS_AGG.HOUR_SID = CLIP_WINDOW_ALIGNED.HOUR_SID
                      AND HOURLY_TVS_AGG.CLIENT_SID = CLIP_WINDOW_ALIGNED.CLIENT_SID 
                      AND HOURLY_TVS_AGG.SESSION_ID = CLIP_WINDOW_ALIGNED.SESSION_ID 
                      AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = CLIP_WINDOW_ALIGNED.CMS_CHANNEL_SID 
                      AND HOURLY_TVS_AGG.CMS_EPISODE_SID = CLIP_WINDOW_ALIGNED.CMS_EPISODE_SID 
                      AND HOURLY_TVS_AGG.CMS_CLIP_SID = CLIP_WINDOW_ALIGNED.CMS_CLIP_SID 
                      AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = CLIP_WINDOW_ALIGNED.CMS_TIMELINE_SID 
                      AND HOURLY_TVS_AGG.APP_SID = CLIP_WINDOW_ALIGNED.APP_SID 
                      AND IFNULL(HOURLY_TVS_AGG.APP_VERSION, 'NA') = IFNULL(CLIP_WINDOW_ALIGNED.APP_VERSION , 'NA')
                      AND HOURLY_TVS_AGG.GEO_SID = CLIP_WINDOW_ALIGNED.GEO_SID 
                      AND HOURLY_TVS_AGG.UTM_SID = CLIP_WINDOW_ALIGNED.UTM_SID 
                      AND IFNULL(HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(CLIP_WINDOW_ALIGNED.VIDEO_SEGMENT_TYPE , 'NA')
                      AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = CLIP_WINDOW_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC ;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                    SET CLIP_WINDOW_ALIGNED_FLAG = FALSE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                    WHERE CLIP_WINDOW_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
$$    
;

CREATE OR REPLACE PROCEDURE "UPDATE_GEO_ALIGNED_FLAG_HOURLY_INACTIVE_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_INACTIVE_TVS_AGG with geo window misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE INACTIVE_GEO_ALIGNED AS 
                    ( SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID
                     JOIN DW_ODIN.CMS_CHANNEL_REGIONS_PARSED_VW CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                     AND UPPER(GEO_DIM.COUNTRY) = UPPER(CMS_CHANNEL_DIM.PARSED_COUNTRY_CODE) 
                     WHERE CMS_CHANNEL_DIM.CHANNEL_ID <> 'vod' AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)

                     UNION

                     SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID
                     JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                     JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                     WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)
                     AND EXISTS (SELECT 1
                                   FROM DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW
                                   WHERE CMS_VODCATEGORIES_PARSED_REGION_VW.EPISODE_ID = CMS_EPISODE_DIM.EPISODE_ID
                                   AND UPPER(GEO_DIM.COUNTRY) = UPPER(CMS_VODCATEGORIES_PARSED_REGION_VW.PARSED_COUNTRY_CODE))

                    UNION

                     SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID
                     JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                     JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                     WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)
                     AND EXISTS (SELECT 1
                                   FROM DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW
                                   WHERE CMS_VODCATEGORIES_PARSED_REGION_VW.SERIES_ID = CMS_EPISODE_DIM.SERIES_ID
                                   AND UPPER(GEO_DIM.COUNTRY) = UPPER(CMS_VODCATEGORIES_PARSED_REGION_VW.PARSED_COUNTRY_CODE))

                                   );`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`    UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                        SET GEO_ALIGNED_FLAG = TRUE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                        FROM INACTIVE_GEO_ALIGNED GEO_ALIGNED
                        WHERE HOURLY_TVS_AGG.HOUR_SID = GEO_ALIGNED.HOUR_SID
                        AND HOURLY_TVS_AGG.CLIENT_SID = GEO_ALIGNED.CLIENT_SID 
                        AND HOURLY_TVS_AGG.SESSION_ID = GEO_ALIGNED.SESSION_ID 
                        AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = GEO_ALIGNED.CMS_CHANNEL_SID 
                        AND HOURLY_TVS_AGG.CMS_EPISODE_SID = GEO_ALIGNED.CMS_EPISODE_SID 
                        AND HOURLY_TVS_AGG.CMS_CLIP_SID = GEO_ALIGNED.CMS_CLIP_SID 
                        AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = GEO_ALIGNED.CMS_TIMELINE_SID 
                        AND HOURLY_TVS_AGG.APP_SID = GEO_ALIGNED.APP_SID 
                        AND HOURLY_TVS_AGG.APP_VERSION = GEO_ALIGNED.APP_VERSION 
                        AND HOURLY_TVS_AGG.GEO_SID = GEO_ALIGNED.GEO_SID 
                        AND HOURLY_TVS_AGG.UTM_SID = GEO_ALIGNED.UTM_SID 
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE = GEO_ALIGNED.VIDEO_SEGMENT_TYPE 
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = GEO_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC ;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    SET GEO_ALIGNED_FLAG = FALSE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                    WHERE GEO_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
$$    
;

CREATE OR REPLACE PROCEDURE "UPDATE_GEO_ALIGNED_FLAG_HOURLY_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_TVS_AGG with geo window misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE GEO_ALIGNED AS 
                    ( SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID
                     JOIN DW_ODIN.CMS_CHANNEL_REGIONS_PARSED_VW CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                     AND UPPER(GEO_DIM.COUNTRY) = UPPER(CMS_CHANNEL_DIM.PARSED_COUNTRY_CODE) 
                     WHERE CMS_CHANNEL_DIM.CHANNEL_ID <> 'vod' AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)

                     UNION

                     SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID
                     JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                     JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                     WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)
                     AND EXISTS (SELECT 1
                                   FROM DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW
                                   WHERE CMS_VODCATEGORIES_PARSED_REGION_VW.EPISODE_ID = CMS_EPISODE_DIM.EPISODE_ID
                                   AND UPPER(GEO_DIM.COUNTRY) = UPPER(CMS_VODCATEGORIES_PARSED_REGION_VW.PARSED_COUNTRY_CODE))

                    UNION

                     SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.GEO_DIM GEO_DIM ON HOURLY_TVS_AGG.GEO_SID = GEO_DIM.GEO_SID
                     JOIN DW_ODIN.CMS_CHANNEL_DIM ON HOURLY_TVS_AGG.CMS_CHANNEL_SID = CMS_CHANNEL_DIM.CMS_CHANNEL_SID
                     JOIN DW_ODIN.CMS_EPISODE_DIM ON HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_DIM.CMS_EPISODE_SID
                     WHERE CMS_CHANNEL_DIM.CHANNEL_ID = 'vod' AND (GEO_ALIGNED_FLAG IS NULL OR GEO_ALIGNED_FLAG = FALSE)
                     AND EXISTS (SELECT 1
                                   FROM DW_ODIN.CMS_VODCATEGORIES_PARSED_REGION_VW
                                   WHERE CMS_VODCATEGORIES_PARSED_REGION_VW.SERIES_ID = CMS_EPISODE_DIM.SERIES_ID
                                   AND UPPER(GEO_DIM.COUNTRY) = UPPER(CMS_VODCATEGORIES_PARSED_REGION_VW.PARSED_COUNTRY_CODE))

                                   );`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`    UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                        SET GEO_ALIGNED_FLAG = TRUE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                        FROM GEO_ALIGNED GEO_ALIGNED
                        WHERE HOURLY_TVS_AGG.HOUR_SID = GEO_ALIGNED.HOUR_SID
                        AND HOURLY_TVS_AGG.CLIENT_SID = GEO_ALIGNED.CLIENT_SID 
                        AND HOURLY_TVS_AGG.SESSION_ID = GEO_ALIGNED.SESSION_ID 
                        AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = GEO_ALIGNED.CMS_CHANNEL_SID 
                        AND HOURLY_TVS_AGG.CMS_EPISODE_SID = GEO_ALIGNED.CMS_EPISODE_SID 
                        AND HOURLY_TVS_AGG.CMS_CLIP_SID = GEO_ALIGNED.CMS_CLIP_SID 
                        AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = GEO_ALIGNED.CMS_TIMELINE_SID 
                        AND HOURLY_TVS_AGG.APP_SID = GEO_ALIGNED.APP_SID 
                        AND IFNULL(HOURLY_TVS_AGG.APP_VERSION, 'NA') = IFNULL(GEO_ALIGNED.APP_VERSION, 'NA') 
                        AND HOURLY_TVS_AGG.GEO_SID = GEO_ALIGNED.GEO_SID 
                        AND HOURLY_TVS_AGG.UTM_SID = GEO_ALIGNED.UTM_SID 
                        AND IFNULL(HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(GEO_ALIGNED.VIDEO_SEGMENT_TYPE , 'NA')
                        AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = GEO_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC ;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                    SET GEO_ALIGNED_FLAG = FALSE, ETL_LOAD_UTC = current_timestamp::timestamp_ntz
                    WHERE GEO_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
$$    
;

CREATE OR REPLACE PROCEDURE "UPDATE_EP_SOURCES_ALIGNED_FLAG_HOURLY_INACTIVE_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_INACTIVE_TVS_AGG with ep source misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE INACTIVE_EP_SOURCES_ALIGNED AS
                    (SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    WHERE EXISTS (
                            SELECT 1 FROM DW_ODIN.CMS_EPISODE_SOURCES_PARSED_VW 
                            WHERE HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_SOURCES_PARSED_VW.CMS_EPISODE_SID
                            AND CMS_CLIP_DIM.CLIP_ID = CMS_EPISODE_SOURCES_PARSED_VW.CLIP_ID            
                    ) 
                     AND (EP_SOURCES_ALIGNED_FLAG IS NULL OR EP_SOURCES_ALIGNED_FLAG = FALSE));`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    SET EP_SOURCES_ALIGNED_FLAG = TRUE
                    FROM INACTIVE_EP_SOURCES_ALIGNED EP_SOURCES_ALIGNED
                    WHERE HOURLY_TVS_AGG.HOUR_SID = EP_SOURCES_ALIGNED.HOUR_SID
                    AND HOURLY_TVS_AGG.CLIENT_SID = EP_SOURCES_ALIGNED.CLIENT_SID 
                    AND HOURLY_TVS_AGG.SESSION_ID = EP_SOURCES_ALIGNED.SESSION_ID 
                    AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = EP_SOURCES_ALIGNED.CMS_CHANNEL_SID 
                    AND HOURLY_TVS_AGG.CMS_EPISODE_SID = EP_SOURCES_ALIGNED.CMS_EPISODE_SID 
                    AND HOURLY_TVS_AGG.CMS_CLIP_SID = EP_SOURCES_ALIGNED.CMS_CLIP_SID 
                    AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = EP_SOURCES_ALIGNED.CMS_TIMELINE_SID 
                    AND HOURLY_TVS_AGG.APP_SID = EP_SOURCES_ALIGNED.APP_SID 
                    AND HOURLY_TVS_AGG.APP_VERSION = EP_SOURCES_ALIGNED.APP_VERSION 
                    AND HOURLY_TVS_AGG.GEO_SID = EP_SOURCES_ALIGNED.GEO_SID 
                    AND HOURLY_TVS_AGG.UTM_SID = EP_SOURCES_ALIGNED.UTM_SID 
                    AND HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE = EP_SOURCES_ALIGNED.VIDEO_SEGMENT_TYPE 
                    AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = EP_SOURCES_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_INACTIVE_TVS_AGG HOURLY_TVS_AGG
                    SET EP_SOURCES_ALIGNED_FLAG = FALSE
                    WHERE EP_SOURCES_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
$$
;

CREATE OR REPLACE PROCEDURE "UPDATE_EP_SOURCES_ALIGNED_FLAG_HOURLY_TVS_AGG"(DAG_ID VARCHAR, TASK_ID VARCHAR, EXECUTION_DATE VARCHAR)
  RETURNS VARCHAR(250)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS 
  $$
    
    var setTimezone = snowflake.createStatement(
      {sqlText: "ALTER SESSION set TIMEZONE = 'UTC'"});
      
    var etlBatchAudit = snowflake.createStatement(
      {sqlText: "INSERT INTO STG.RPT_ETL_BATCH_AUDIT(DAG_ID, TASK_ID, EXECUTION_DATE, TASK) SELECT '" + DAG_ID + "','" + TASK_ID + "','" + EXECUTION_DATE + "', 'ETL to update HOURLY_TVS_AGG with ep source misalignment flag' FROM DUAL"});                                  
    
     try {
        setTimezone.execute();
        etlBatchAudit.execute();
        
        var batchID = snowflake.createStatement(
            {sqlText: "select MAX(BATCH_ID) BATCH_ID from stg.RPT_ETL_BATCH_AUDIT WHERE TASK_ID = '" + TASK_ID + "' AND DAG_ID = '" + DAG_ID + "' AND EXECUTION_DATE = '" + EXECUTION_DATE + "'"});
        batchIDres = batchID.execute();
        batchIDres.next();
        batch_ID = batchIDres.getColumnValue(1);

        var etlLoad = snowflake.createStatement(
          {sqlText:`CREATE OR REPLACE TEMPORARY TABLE EP_SOURCES_ALIGNED AS
                    (SELECT HOURLY_TVS_AGG.*
                    FROM RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                     JOIN DW_ODIN.CMS_CLIP_DIM ON HOURLY_TVS_AGG.CMS_CLIP_SID = CMS_CLIP_DIM.CMS_CLIP_SID
                    WHERE EXISTS (
                            SELECT 1 FROM DW_ODIN.CMS_EPISODE_SOURCES_PARSED_VW 
                            WHERE HOURLY_TVS_AGG.CMS_EPISODE_SID = CMS_EPISODE_SOURCES_PARSED_VW.CMS_EPISODE_SID
                            AND CMS_CLIP_DIM.CLIP_ID = CMS_EPISODE_SOURCES_PARSED_VW.CLIP_ID            
                    ) 
                     AND (EP_SOURCES_ALIGNED_FLAG IS NULL OR EP_SOURCES_ALIGNED_FLAG = FALSE));`});
        etlLoad.execute(); 
        
        var etlLoad2 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                    SET EP_SOURCES_ALIGNED_FLAG = TRUE
                    FROM EP_SOURCES_ALIGNED EP_SOURCES_ALIGNED
                    WHERE HOURLY_TVS_AGG.HOUR_SID = EP_SOURCES_ALIGNED.HOUR_SID
                    AND HOURLY_TVS_AGG.CLIENT_SID = EP_SOURCES_ALIGNED.CLIENT_SID 
                    AND HOURLY_TVS_AGG.SESSION_ID = EP_SOURCES_ALIGNED.SESSION_ID 
                    AND HOURLY_TVS_AGG.CMS_CHANNEL_SID = EP_SOURCES_ALIGNED.CMS_CHANNEL_SID 
                    AND HOURLY_TVS_AGG.CMS_EPISODE_SID = EP_SOURCES_ALIGNED.CMS_EPISODE_SID 
                    AND HOURLY_TVS_AGG.CMS_CLIP_SID = EP_SOURCES_ALIGNED.CMS_CLIP_SID 
                    AND HOURLY_TVS_AGG.CMS_TIMELINE_SID = EP_SOURCES_ALIGNED.CMS_TIMELINE_SID 
                    AND HOURLY_TVS_AGG.APP_SID = EP_SOURCES_ALIGNED.APP_SID 
                    AND IFNULL(HOURLY_TVS_AGG.APP_VERSION, 'NA') = IFNULL(EP_SOURCES_ALIGNED.APP_VERSION , 'NA')
                    AND HOURLY_TVS_AGG.GEO_SID = EP_SOURCES_ALIGNED.GEO_SID 
                    AND HOURLY_TVS_AGG.UTM_SID = EP_SOURCES_ALIGNED.UTM_SID 
                    AND IFNULL(HOURLY_TVS_AGG.VIDEO_SEGMENT_TYPE, 'NA') = IFNULL(EP_SOURCES_ALIGNED.VIDEO_SEGMENT_TYPE , 'NA')
                    AND HOURLY_TVS_AGG.VIDEO_SEGMENT_BEGIN_UTC = EP_SOURCES_ALIGNED.VIDEO_SEGMENT_BEGIN_UTC;`});
        res2 = etlLoad2.execute(); 
        res2.next();
        row_updated2 = res2.getColumnValue(1);
        
        var etlLoad3 = snowflake.createStatement(
          {sqlText:`UPDATE RPT.HOURLY_TVS_AGG HOURLY_TVS_AGG
                    SET EP_SOURCES_ALIGNED_FLAG = FALSE
                    WHERE EP_SOURCES_ALIGNED_FLAG IS NULL;`});
        res3 = etlLoad3.execute();        
        res3.next();
        row_updated3 = res3.getColumnValue(1);
        
        row_updated = row_updated2 + row_updated3;
                
        var updateEtlBatchAuditSuccess = snowflake.createStatement(
          {sqlText: `MERGE INTO STG.RPT_ETL_BATCH_AUDIT
                      USING 
                      (select ` + batch_ID + ` BATCH_ID from DUAL ) STG_ETL_BATCH_AUDIT
                      ON STG_ETL_BATCH_AUDIT.BATCH_ID = RPT_ETL_BATCH_AUDIT.BATCH_ID
                      WHEN MATCHED
                      THEN UPDATE SET
                      RECORDS_UPDATED = `
                      + row_updated
                      + `, STATUS = 'Succeeded.' 
                      , QUERY_ID = (SELECT LAST_QUERY_ID(-1) || ' | ' || LAST_QUERY_ID(-2)) `});
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
  $$  
  ;