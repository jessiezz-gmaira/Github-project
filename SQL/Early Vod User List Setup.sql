set  (start_date, end_date) = ('2022-09-15'::date, '2022-10-15'::date);

USE DATABASE SANDBOX;
USE SCHEMA ANALYSIS_PRODUCT;

CREATE OR REPLACE TABLE VodUsageEarlyVodUsers_SH_20230208 AS
WITH cfeed AS --find users who used cfeed at least once and categorize them as cfeed users
(
    SELECT
    client_id,
    c_feed
    FROM
    (
        SELECT
        client_id,
        c_feed,
        ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY C_FEED ASC) AS ranks
        FROM
        (
            SELECT
            ue.client_id,
            CASE
                WHEN utm_medium = 'searchintegration' AND ue.app_name = 'roku' THEN 'C Feed'
                WHEN utm_medium IN ('textsearch', 'ossearch', 'deeplink') AND ue.app_name = 'androidmobile' THEN 'C Feed'
                WHEN utm_medium IN ('textsearch', 'ossearch', 'deeplink','apptile') AND ue.app_name = 'androidtv' THEN 'C Feed'
                ELSE 'Non C Feed'
                END AS c_feed
            FROM
            ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 AS ue
            INNER JOIN 
            ODIN_PRD.RPT.ALL_CLIENT_FIRST_SEEN fs 
            ON
            ue.client_id = fs.client_id AND date_trunc('day', client_first_seen_utc) between $start_date and $end_date
            WHERE
            date_trunc('day', time_stamp) between $start_date and $end_date
            AND
            ue.app_name IN ('androidtv','androidmobile','roku','firetv')
            AND
            country = 'US'
            GROUP BY 1,2
        )
    )
    WHERE
    ranks = 1
),

vod_users AS
(
    SELECT
    *,
    datediff(day, seen_date, first_vod_date) AS vod_date_diff --find the date difference from first seen to first VOD
    FROM
    (
        SELECT
        *,
        row_number() OVER(partition by client_id ORDER BY first_vod_date asc) AS vod_rank  -- find the first day a new users used VOD
        FROM
        (
            SELECT
            ag.client_id,
            date_trunc('day',fs.client_first_seen_utc) AS seen_date,
            date_trunc('day',utc) AS first_vod_date,
            SUM(total_viewing_minutes) AS tvm
            FROM 
            ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG ag
            INNER JOIN 
            ODIN_PRD.RPT.ALL_CLIENT_FIRST_SEEN fs 
            ON
            ag.client_id = fs.client_id AND date_trunc('day', client_first_seen_utc) between $start_date and $end_date
            INNER JOIN 
            ODIN_PRD.DW_ODIN.HOUR_DIM h 
            ON h.hour_sid = ag.hour_sid
            WHERE
            date_trunc('day', utc) between $start_date and $end_date
            AND
            channel_id = 'vod'
            AND
            ag.app_name IN ('androidtv','androidmobile','roku','firetv')
            AND
            country = 'US'
            GROUP BY 1,2,3
        )
    )
    WHERE
    vod_rank = 1
)

SELECT
client.client_id,
CASE WHEN vu.client_id is not null THEN true ELSE false END AS vod_users,
client.seen_date,
CASE WHEN vu.client_id is not null THEN vu.vod_date_diff else client.vod_date_diff END AS vod_date_diff,
CASE WHEN vu.client_id is not null THEN vu.first_vod_date else client.first_vod_date END AS first_vod_date,
c_feed,
client.tvm AS client_tvm
FROM
(
    SELECT
    *,
    datediff(day, seen_date, first_vod_date) AS vod_date_diff --find the date difference from first seen to first VOD
    FROM
    (
        SELECT
        *,
        row_number() OVER(partition by client_id ORDER BY first_vod_date asc) AS vod_rank  -- find the first day a new users used VOD
        FROM
        (
            SELECT
            ag.client_id,
            date_trunc('day',fs.client_first_seen_utc) AS seen_date,
            date_trunc('day',utc) AS first_vod_date,
            SUM(total_viewing_minutes) AS tvm
            FROM 
            ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG ag
            INNER JOIN 
            ODIN_PRD.RPT.ALL_CLIENT_FIRST_SEEN fs 
            ON
            ag.client_id = fs.client_id AND date_trunc('day', client_first_seen_utc) between $start_date and $end_date
            INNER JOIN 
            ODIN_PRD.DW_ODIN.HOUR_DIM h 
            ON h.hour_sid = ag.hour_sid
            WHERE
            date_trunc('day', utc) between $start_date and $end_date
            AND
            ag.app_name IN ('androidtv','androidmobile','roku','firetv')
            AND
            country = 'US'
            GROUP BY 1,2,3
        )
    )
    WHERE
    vod_rank = 1
) AS client
INNER JOIN
cfeed 
ON
cfeed.client_id = client.client_id
LEFT JOIN
vod_users AS vu
ON
vu.client_id = client.client_id
order by vod_date_diff asc;
