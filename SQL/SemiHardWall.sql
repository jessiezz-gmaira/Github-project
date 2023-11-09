SET
    START_DATE = '2023-06-07';
SET
    end_date = '2023-06-25';
SET
    exp_id = '24489200019';
SET
    app_name = 'tvos';
with optimizely as (
        select
            distinct VARIATION_ID,
            visitor_id
        from
            "OPTIMIZELY"."PUBLIC"."PLUTOTV_DECISIONS"
        where
            date_trunc(
                'day',
                convert_timezone('UTC', 'America/New_York', TIMESTAMP)
            ) between $start_date
            and $end_date
            and EXPERIMENT_ID = $exp_id
        group by
            1,
            2
    ),
    event_rank as (
        select
            session_id,
            client_id,
            user_sid,
            event_name,
            time_stamp
        from
            ODIN_PRD.RPT.BI_USER_ENGAGEMENT360
        where
            date_trunc(
                'day',
                convert_timezone('UTC', 'America/New_York', time_stamp)
            ) between $start_date
            and $end_date
            and app_name = $app_name
            and event_name in (
                'appLaunch',
                'channelGuideRequest',
                'sessionReset',
                'clipStart',
                'episodeStart',
                'signUpSuccessful',
                'signInSuccessful'
            )
            and UTM_Medium != 'deeplink'
    ),
    registrants as (
        select
            distinct client_id --,date_trunc('DAY', TIME_STAMP) as Reg_Dates
        from
            event_rank
        where
            EVENT_NAME in ('signUpSuccessful', 'signInSuccessful') --and session_id in (select distinct session_id from first_session where session_rank = 1)
    ),
    reg_wall as (
        (
            select
                distinct client_ID --,date_trunc('DAY', time_stamp) as Reg_wall_Dates
            from
                ODIN_PRD.RPT.BI_USER_ENGAGEMENT360
            where
                date_trunc(
                    'day',
                    convert_timezone('UTC', 'America/New_York', time_stamp)
                ) between '2023-06-14'
                and $end_date
                and app_name = $app_name
                and event_name in ('pageView')
                and screen_name in (
                    'registrationwallsoftprompt',
                    'registrationwallhardprompt'
                )
        )
        union
            (
                select
                    distinct client_id --,date_trunc('DAY', EVENT_OCCURRED_UTC) as Reg_wall_Dates
                from
                    "SANDBOX"."PRODUCT"."REGISTRATION_TESTING_DATA_V2" a
                where
                    SCREENID not in ('10084')
                    and date_trunc(
                        'day',
                        convert_timezone('UTC', 'America/New_York', EVENT_OCCURRED_UTC)
                    ) >= $start_date
                    and date_trunc(
                        'day',
                        convert_timezone('UTC', 'America/New_York', EVENT_OCCURRED_UTC)
                    ) < '2023-06-14'
            )
    ),
    new_users as (
        select
            distinct CLIENT_ID --    ,date_trunc('DAY', CLIENT_FIRST_SEEN_UTC) as First_Seen_Dates
        from
            "ODIN_PRD"."RPT"."ALL_CLIENT_FIRST_SEEN"
        where
            date_trunc(
                'day',
                convert_timezone('UTC', 'America/New_York', CLIENT_FIRST_SEEN_UTC)
            ) between $start_date
            and $end_date
            and app_name = $app_name
    ),
    tvmtable as (
        select
            client_id --, date_trunc('DAY', VIDEO_SEGMENT_BEGIN_UTC) as ActiveDates
,
            sum(TOTAL_VIEWING_MINUTES) TVM,
            count (distinct session_id) as Sessions
        from
            ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG_US a
        where
            date_trunc(
                'day',
                convert_timezone(
                    'UTC',
                    'America/New_York',
                    VIDEO_SEGMENT_BEGIN_UTC
                )
            ) between $start_date
            and $end_date
            and app_name in ($app_name)
        group by
            1 --,2
    ),
    impressions as (
        select
            CLIENT_ID --, date_trunc('DAY', HOUR_TIMESTAMP_SERVER) as impression_Dates
,
            count(
                case
                    when AD_SOURCE = 'fw' then CM_IMPRESSION_COUNT
                end
            ) Paid_Impressions,
            count(CM_IMPRESSION_COUNT) as Impressions
        from
            "HIST"."RPT"."HOURLY_CM_REALTIME_EVENT"
        where
            date_trunc(
                'day',
                convert_timezone('UTC', 'America/New_York', HOUR_TIMESTAMP_SERVER)
            ) between $start_date
            and $end_date
        group by
            1 --,2
    ),
    rev as (
        select
            PLUTO_DEVICEID,   case when SOLD_TYPE_CLEAN in ('Direct Response','Direct Sales') then 'fw_direct'
            when SOLD_TYPE_CLEAN = 'Indirect Sales' then 'fw_indirect' else 'others' end as SOLD_TYPE_CLEAN,
            --SOLD_TYPE_CLEAN,
            sum(REVENUE_SUM) as rev,
            sum(imps_sum) as imp,
            sum(paid_imps_sum) as paid_imp
        from
            SANDBOX.ANALYSIS_OPERATIONS.LZ_REVENUE_NEW --where PLUTO_DEVICEID = '8d2397cd-7dcc-4423-b540-e797f457105c_bab5d32ac40818ee'
        where
            et_date >= $start_date
            and et_date <= $end_date
        group by
            1,2
    )
select
    VARIATION_ID --, Reg_wall_Dates
,SOLD_TYPE_CLEAN,
    count(distinct a.client_id) reg_wall_clients,
    count(distinct b.client_id) registered_clients,
    count(distinct lower(e.client_id)) active_clients,
    sum(Impressions) Impressions,
    sum(Paid_Impressions) Paid_Impressions,

    sum(rev) rev,
    sum(imp) as imp_rev_table,
    sum(paid_imp) as paid_imp_rev_table,
    stddev(ifnull(rev, 0)) as stddev_rev,
    stddev(
        case
            when e.client_id is not null then ifnull(rev, 0)
        end
    ) as stddev_rev_active
from
    reg_wall a
    left join registrants b on a.client_id = b.client_id --and a.Reg_wall_Dates = b.reg_dates
    inner join optimizely c on sha2(lower(a.client_id)) = c.visitor_id
    inner join new_users d on a.client_id = d.client_id
    left join tvmtable e on lower(a.client_id) = lower(e.client_id) --and b.reg_dates = e.ActiveDates
    left join impressions f on lower(a.client_id) = lower(f.client_id) --and a.Reg_wall_Dates = f.impression_Dates
    left join rev r on lower(a.client_id) = lower(r.PLUTO_DEVICEID) --and b.reg_dates = e.ActiveDates
where
    VARIATION_ID != '24480010016' --    SANDBOX.PRODUCT.Top_perc_June_Reg_test
group by
    1 , 2
order by
    1