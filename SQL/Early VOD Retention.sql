WITH user_filter AS (
    SELECT
    client_id,
    vod_date_diff,
    CASE WHEN (case when vod_date_diff = 0 THEN SUM(client_tvm) END) >= 30 THEN true
        WHEN vod_date_diff != 0 THEN true
        Else false END AS day_0_engaged
    FROM
    SANDBOX.ANALYSIS_PRODUCT.VodUsageEarlyVodUsers_SH_20230208
    GROUP BY 1,2
),

ret_setup AS
(
    SELECT
    ue.client_id,
    CASE 
        WHEN datediff(day, first_vod_date, time_stamp) >= 0 AND datediff(day, first_vod_date, time_stamp) < 30 THEN 'retained' END as first_month,
    CASE    
        WHEN datediff(day, first_vod_date, time_stamp) > 30 THEN 'retained' END AS second_month
    FROM
    ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 AS ue
    INNER JOIN
    SANDBOX.ANALYSIS_PRODUCT.VodUsageEarlyVodUsers_SH_20230208 AS vu
    ON
    ue.client_id = vu.client_id
    WHERE
    date_trunc('day', time_stamp) between '2022-09-15' and '2022-12-15' -- covers 2 month of retention post VOD start
    AND
    ue.app_name IN ('androidtv','androidmobile','roku','firetv')
    order by client_id asc
),

retention AS 
(
    SELECT
        ret_setup.client_id,
        day_0_engaged,
        MAX(first_month) AS first_month_retention,
        MAX(second_month) AS second_month_retention
    FROM
    ret_setup
    INNER JOIN 
    user_filter AS uf
    ON
    ret_setup.client_id = uf.client_id
    GROUP BY 1,2
    order by 1
),

freq_setup AS 
(
    SELECT
    ue.client_id,
    CASE WHEN datediff(day, first_vod_date, time_stamp) <= 30 THEN TRUE END AS first_month,
    date_trunc('day', time_stamp) AS date
    FROM
    ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 AS ue
    INNER JOIN
    SANDBOX.ANALYSIS_PRODUCT.VodUsageEarlyVodUsers_SH_20230208 AS vu
    ON
    ue.client_id = vu.client_id
    WHERE
    date_trunc('day', time_stamp) between '2022-09-15' and '2022-11-15' -- Covers 1 month of frequency post VOD start
    AND
    ue.app_name IN ('androidtv','androidmobile','roku','firetv')
    order by client_id asc
),

frequency AS 
(
    SELECT
    freq_setup.client_id,
    day_0_engaged,
    COUNT(distinct date) AS first_month_visit_freq
    FROM
    freq_setup
    INNER JOIN 
    user_filter AS uf
    ON
    freq_setup.client_id = uf.client_id
    WHERE
    first_month = TRUE
    GROUP BY 1,2
    ORDER BY client_id
),

ASD_setup AS
(
    SELECT
    ag.client_id,
    CASE WHEN datediff(day, first_vod_date, utc) <= 30 THEN TRUE END AS first_month,
    SUM(CASE WHEN datediff(day, first_vod_date, utc) <= 30 THEN total_viewing_minutes END) AS first_month_asd_tvm,
    COUNT(distinct CASE WHEN datediff(day, first_vod_date, utc) <= 30 THEN session_id END) AS first_month_asd_session_counts
    FROM
    ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG AS ag
    INNER JOIN
    SANDBOX.ANALYSIS_PRODUCT.VodUsageEarlyVodUsers_SH_20230208 AS vu
    ON
    ag.client_id = vu.client_id
    INNER JOIN 
    ODIN_PRD.DW_ODIN.HOUR_DIM h 
    ON 
    h.hour_sid = ag.hour_sid 
    WHERE
    date_trunc('day', utc) between '2022-09-15' and '2022-11-15' --covers 1 month of ASD post VOD start
    AND
    ag.app_name IN ('androidtv','androidmobile','roku','firetv')
    GROUP BY 1,2
    order by ag.client_id asc
),

ASD AS 
(
    SELECT
    ASD_setup.client_id,
    day_0_engaged,
    first_month_asd_tvm,
    first_month_asd_session_counts
    FROM
    ASD_setup
    INNER JOIN 
    user_filter AS uf
    ON
    ASD_setup.client_id = uf.client_id
    WHERE
    first_month = TRUE
),


Data_list AS 
(
    SELECT
    ag.client_id,
    vod_users,
    ret.day_0_engaged,
    client_tvm,
    c_feed,
    vod_date_diff,
    first_month_retention,
    second_month_retention,
    first_month_visit_freq,
    first_month_asd_tvm,
    first_month_asd_session_counts,
    app_name,
    SUM(CASE WHEN datediff(day, first_vod_date, utc) < 30 AND ag.channel_id = 'vod' THEN total_viewing_minutes END) AS first_month_vod_tvm,
    SUM(CASE WHEN datediff(day, first_vod_date, utc) < 30 THEN total_viewing_minutes END) AS first_month_tvm,
    SUM(CASE WHEN datediff(day, first_vod_date, utc) >= 30 AND datediff(day, first_vod_date, utc) < 60 AND ag.channel_id = 'vod' THEN total_viewing_minutes END) AS second_month_vod_tvm,
    SUM(CASE WHEN datediff(day, first_vod_date, utc) >= 30 AND datediff(day, first_vod_date, utc) < 60 THEN total_viewing_minutes END) AS second_month_tvm,
    SUM(CASE WHEN datediff(day, first_vod_date, utc) >= 60 AND datediff(day, first_vod_date, utc) < 90 AND ag.channel_id = 'vod' AND ag.channel_id = 'vod' THEN total_viewing_minutes END) AS third_month_vod_tvm,
    SUM(CASE WHEN datediff(day, first_vod_date, utc) >= 60 AND datediff(day, first_vod_date, utc) < 90 THEN total_viewing_minutes END) AS third_month_tvm
    FROM
    ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG AS ag
    INNER JOIN
    SANDBOX.ANALYSIS_PRODUCT.VodUsageEarlyVodUsers_SH_20230208 AS vu
    ON
    ag.client_id = vu.client_id
    INNER JOIN 
    ODIN_PRD.DW_ODIN.HOUR_DIM h 
    ON 
    h.hour_sid = ag.hour_sid 
    INNER JOIN
    retention AS ret
    ON
    ag.client_id = ret.client_id
    INNER JOIN
    frequency AS freq
    ON
    ag.client_id = freq.client_id
    INNER JOIN
    ASD
    ON
    ag.client_id = ASD.client_id
    WHERE
    date_trunc('day', utc) between '2022-09-15' and '2023-01-15'
    AND
    country = 'US'
    AND
    ag.app_name IN ('androidtv','androidmobile','roku','firetv')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    ORDER BY client_id asc
)

SELECT
vod_date_diff,
-- c_feed,
app_name,
COUNT(second_month_retention) AS retention_count,
COUNT(DISTINCT client_id) AS user_counts,
SUM(first_month_visit_freq)/user_counts AS avg_freq_per_user,
SUM(first_month_asd_tvm)/SUM(first_month_asd_session_counts) AS ASD,
retention_count/user_counts AS percent_retained
FROM
Data_list
WHERE
vod_users = true
-- and
-- day_0_engaged = true
GROUP BY 1,2;


SELECT
vod_users,
COUNT(distinct client_id) AS user_counts
FROM
SANDBOX.ANALYSIS_PRODUCT.VodUsageEarlyVodUsers_SH_20230208
GROUP BY 1