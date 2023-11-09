-- CREATE OR REPLACE TABLE SANDBOX.ANALYSIS_PRODUCT.SEARCHES_GT_051923_TEST2 AS
WITH BASE AS //Getting all search results for sample period
(
    SELECT distinct
      CONVERT_TIMEZONE('UTC', 'America/New_York',request_utc) as search_dt
    , TO_TIME(substring(search_dt,12,8),'HH24:MI:SS') AS search_ts
    , app_name
    , session_id
    , lower(query) as search
    , length(search) as search_length

    FROM "ODIN_PRD"."RPT"."TRUE_SEARCHES"
    WHERE 1=1
    -- AND date_trunc('day', etl_load_utc) between '2023-01-31' and '2023-02-09'
    -- and search_dt between '2023-02-01' and '2023-02-07'
    and date_trunc('month',search_dt) = '2023-04-01'
    and lower(country) = 'us'
    and lower(app_name) = 'roku'
    -- order by session_id, search_dt
    -- limit 10000
)

, S1 AS ( //Determining the time difference between searches (checking for grouped searches)
    SELECT *
    , timediff('second', search_dt, lead(search_dt) over (partition by session_id order by search_dt asc)) delta_lead
    , timediff('second', search_dt, lag(search_dt) over (partition by session_id order by search_dt asc)) delta_lag
    FROM BASE
    -- ORDER BY session_id, search_dt;
)

, S2 AS ( // Initial filtering of pre-final search terms [ cr, creed, breaking, breaking bad] -> [creed, breaking bad]. 5 minute search break is a new search

    SELECT *
    ,cast(regexp_replace(cast(row_number () over (partition by session_id, substring(search,1,1) order by search_length desc, search_dt desc) as varchar), '[2-9]','0') as int) as rn
    ,case when delta_lead > 300 then 1 else 0 end as rn2

    FROM S1
    -- ORDER BY session_id, search_dt;
)

, SESSION_CK AS //Need to make sure search sample using did have sessions in ag table, there are some search sessions that do not appear in ag table so removing those
(
    -- select distinct client_id, session_id from SANDBOX.ANALYSIS_CONTENT.ASSET_LEVEL_VIEWERSHIP_TEST
    select distinct client_id, session_id from SANDBOX.ANALYSIS_PRODUCT.ASSET_LEVEL_VIEWERSHIP_GT_051923
)


, S3 AS // determining a search resulted in an action if the user changed and watched something within 3 minutes of the search. Removing duplicate joins on search
(
    SELECT
        ck.client_id as client_id_ck,
        alv.client_id,
        S2.search_dt,
        S2.SESSION_ID,
        S2.SEARCH,
        alv.start_date,
        alv.start_ts_est,
        alv.channel_name,
        alv.content_type,
        alv.series_name
        , row_number() over (partition by s2.session_id, s2.search_dt order by timediff('second', s2.search_ts, alv.start_ts_est)) rn3
        , row_number() over (partition by s2.session_id, s2.search_dt order by case when channel_name ='VOD' then 1 else 2 end, timediff('second', s2.search_ts, alv.start_ts_est)) rn4

    FROM S2
    -- LEFT JOIN SANDBOX.ANALYSIS_CONTENT.ASSET_LEVEL_VIEWERSHIP_TEST alv
    LEFT JOIN SANDBOX.ANALYSIS_PRODUCT.ASSET_LEVEL_VIEWERSHIP_GT_051923 alv
        ON S2.session_id = alv.session_id
        AND date_trunc('day', search_dt) = alv.start_date
        AND alv.start_ts_est >= s2.search_ts
        -- AND timediff('second', s2.search_ts, alv.start_ts_est) < 180
        AND timediff('second', s2.search_ts, alv.start_ts_est) < 600

    LEFT JOIN SESSION_CK ck
        ON ck.session_id = s2.session_id

    where 1=1
        and (rn = 1 or rn2 =1)
        and client_id_ck is not null
    -- order by session_id, search_dt
)

-- ,S4 AS ( //removing duplicate joins on alv asset

SELECT
    *
    , case when channel_name is null then null else row_number() over (partition by session_id, start_ts_est order by search_dt desc) end as rn5
from S3
where 1=1
  and rn4 = 1

    qualify (rn5 is null or rn5 = 1)

    order by session_id, search_dt
;
-- gathering the daily TVMs per VOD searchced content
SELECT
    s.start_date,
    sum(a.tvms)
FROM
    SANDBOX.ANALYSIS_PRODUCT.SEARCHES_GT_051923_TEST2 s
    JOIN SANDBOX.ANALYSIS_PRODUCT.ASSET_LEVEL_VIEWERSHIP_GT_051923 a on a.client_id = s.client_id
                                                                    and a.session_id = s.session_id
                                                                    and a.start_date = s.start_date
                                                                    and a.start_ts_est = s.start_ts_est
                                                                    and a.series_name = s.series_name
WHERE 1=1
    AND s.channel_name = 'VOD'
GROUP BY 1
;
