 --List the users by their daily TVM for further indepth analysis in python.
 SELECT
    app_name,
    -- date,
    vod_date_diff,
    day_since_first_vod_seen,
    -- c_feed,
    CASE 
        WHEN SUM(CASE WHEN channel_id = 'vod' THEN total_viewing_minutes END) is null THEN 0
        ELSE SUM(CASE WHEN channel_id = 'vod' THEN total_viewing_minutes END) END AS daily_vod_total_tvm,
    SUM(total_viewing_minutes) AS daily_total_tvm,
    COUNT(DISTINCT client_id) AS total_users,
    count(distinct session_id) AS daily_sessions,
    daily_vod_total_tvm/daily_total_tvm AS percent_vod,
    daily_vod_total_tvm/total_users AS percent_user_vod
FROM
(
    SELECT
        ag.client_id,
        ag.app_name,
        date_trunc('day', utc) AS date,
        vod_date_diff,
        datediff(day, first_vod_date, utc) AS day_since_first_vod_seen,
        channel_id,
        c_feed,
        total_viewing_minutes,
        session_id
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
    date_trunc('day', utc) between '2022-09-15' and '2022-11-15'
    AND
    country = 'US'
    AND
    ag.app_name IN ('androidtv','androidmobile','roku','firetv')
    AND
    vod_users = true
    ORDER BY client_id asc
)
WHERE
day_since_first_vod_seen >= 0
and
day_since_first_vod_seen < 30
GROUP BY 1,2,3
ORDER BY vod_date_diff asc, day_since_first_vod_seen asc;
