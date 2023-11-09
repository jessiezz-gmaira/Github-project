TOTAL CLIENTS AND SESSIONS


-- RUN these four lines of SQL separately from the query below
-- global variables
SET start_date = '2023-06-08';
SET end_date = '2023-06-11';
SET exp_id = '24489200019';
SET app_name = 'tvos';


--RUN from here to bottom of SQL after you've run the above four lines
-- -get hashed client id for variation
with optimizely as
(
  select distinct VARIATION_ID,visitor_id
 from "OPTIMIZELY"."PUBLIC"."PLUTOTV_DECISIONS"
 where TIMESTAMP >= $start_date
    and TIMESTAMP <= $end_date
   and EXPERIMENT_ID = $exp_id
  group by 1,2
)
-- get active clients, sessions, TVM
,
tvmtable as
(
  select  client_id
      , date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',VIDEO_SEGMENT_BEGIN_UTC::timestamp_ntz)) as ActiveDates     
   from ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG_US a
   where VIDEO_SEGMENT_BEGIN_UTC >= $start_date
        and VIDEO_SEGMENT_BEGIN_UTC <= $end_date
        and app_name in ($app_name)
        group by 1,2
)
-- get all users client id
, all_users as
(
    select distinct client_id
        , date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',TIME_STAMP::timestamp_ntz)) as All_users_Dates
        , count (distinct session_id) as Sessions
    from ODIN_PRD.RPT.BI_USER_ENGAGEMENT360
    where TIME_STAMP >= $start_date
        and TIME_STAMP <= $end_date
        and app_name = 'tvos'
    group by 1,2
)
-- get new registered users client id
, registrants as
(
    select distinct client_id
        ,date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',TIME_STAMP::timestamp_ntz)) as Reg_Dates
    from ODIN_PRD.RPT.BI_USER_ENGAGEMENT360
    where EVENT_NAME in ('signUpSuccessful','signInSuccessful')
        and TIME_STAMP >= $start_date
        and TIME_STAMP <= $end_date
)
-- get all clients (new users)
, new_users as
(
 select distinct CLIENT_ID
     ,date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',CLIENT_FIRST_SEEN_UTC::timestamp_ntz)) as First_Seen_Dates
 from "ODIN_PRD"."RPT"."ALL_CLIENT_FIRST_SEEN"
  where CLIENT_FIRST_SEEN_UTC >= $start_date
    and CLIENT_FIRST_SEEN_UTC < $end_date
)

-- join all the above tables (active users/KPIs in experiment who are new)
, opt_sessions as
(
    select agg.*, VARIATION_ID, regs.CLIENT_ID as Reg_Clients
    from all_users agg
    inner join optimizely opt2
        on sha2(lower(agg.CLIENT_ID), 256) = opt2.visitor_id
    left join tvmtable tvmt
        on agg.CLIENT_ID = tvmt.CLIENT_ID
        and agg.all_users_Dates = tvmt.ActiveDates
    inner join new_users nu
        on agg.CLIENT_ID = nu.CLIENT_ID
        and agg.all_users_Dates = nu.First_Seen_Dates
    left join registrants regs
        on agg.CLIENT_ID = regs.CLIENT_ID
        and agg.all_users_Dates = regs.Reg_Dates
)
select all_users_Dates,
    VARIATION_ID,
    count(distinct a.CLIENT_ID) as TOTAL_CLIENTS,
    sum(Sessions) as TOTAL_SESSIONS

from opt_sessions a
group by 1,2
order by 1

-------
-------
-------
-------




Active Clients, Sessions, TVMs, Registrants

-- RUN THESE FOUR LINES BEFORE RUNNING THE SQL BELOW
-- global variables
SET start_date = '2023-06-08';
SET end_date = '2023-06-11';
SET exp_id = '24489200019';
SET app_name = 'tvos';


-- RUN FROM HERE TO BOTTOM OF SQL AFTER RUNNING THE FOUR LINES ABOVE
-- -get hashed client id for variation
with optimizely as
(
  select distinct VARIATION_ID,visitor_id
 from "OPTIMIZELY"."PUBLIC"."PLUTOTV_DECISIONS"
 where TIMESTAMP >= $start_date
    and TIMESTAMP <= $end_date
   and EXPERIMENT_ID = $exp_id
  group by 1,2
)
-- get active clients, sessions, TVM
,
tvmtable as
(
  select  client_id
      , date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',VIDEO_SEGMENT_BEGIN_UTC::timestamp_ntz)) as ActiveDates
      , sum(TOTAL_VIEWING_MINUTES) as TVM
      ,count (distinct SESSION_ID) as sessions
   from ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG_US a
   where VIDEO_SEGMENT_BEGIN_UTC >= $start_date
        and VIDEO_SEGMENT_BEGIN_UTC <= $end_date
        and app_name in ($app_name)
        group by 1,2
)
-- get new registered users client id
, registrants as
(
    select distinct client_id,date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',TIME_STAMP::timestamp_ntz)) as Reg_Dates
    from ODIN_PRD.RPT.BI_USER_ENGAGEMENT360
    where EVENT_NAME in ('signUpSuccessful','signInSuccessful')
        and TIME_STAMP >= $start_date
        and TIME_STAMP <= $end_date
)
-- get all clients (new users)
, new_users as
(
 select distinct CLIENT_ID,date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',CLIENT_FIRST_SEEN_UTC::timestamp_ntz)) as First_Seen_Dates
 from "ODIN_PRD"."RPT"."ALL_CLIENT_FIRST_SEEN"
  where CLIENT_FIRST_SEEN_UTC >= $start_date
    and CLIENT_FIRST_SEEN_UTC < $end_date
)

-- join all the above tables (active users/KPIs in experiment who are new)
, opt_sessions as
(
    select agg.*, VARIATION_ID, regs.CLIENT_ID as Reg_Clients
    from tvmtable agg
    inner join optimizely opt2
        on sha2(lower(agg.CLIENT_ID), 256) = opt2.visitor_id
    inner join new_users nu
        on agg.CLIENT_ID = nu.CLIENT_ID
        and agg.ActiveDates = nu.First_Seen_Dates
    left join registrants regs
        on lower(agg.CLIENT_ID) = lower(regs.CLIENT_ID)
        and agg.ActiveDates = regs.Reg_Dates
)
select ActiveDates,
    VARIATION_ID,
    count(distinct a.CLIENT_ID) as NO_ACTIVE_CLIENTS,
    count(distinct Reg_Clients) as NO_REGISTRANTS,
    sum(Sessions) as NO_ACTIVE_SESSIONS,
    sum(a.TVM) as TVMs
from opt_sessions a
group by 1,2











------
------
------
------
---Impressions by source, variant by day


-- RUN these four lines of SQL separately from the query below
--global variables 
SET start_date = '2023-06-08';
SET end_date = '2023-06-11';
SET exp_id = '24489200019';
SET app_name = 'tvos';


--RUN from here to bottom of SQL after you've run the above four lines
with optimizely as
(
  select distinct VARIATION_ID,visitor_id
 from "OPTIMIZELY"."PUBLIC"."PLUTOTV_DECISIONS"
 where TIMESTAMP >= $start_date
    and TIMESTAMP <= $end_date
   and EXPERIMENT_ID = $exp_id
  group by 1,2
)

-- get active clients, sessions, TVM
,
tvmtable as
(
  select  client_id
      , date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',VIDEO_SEGMENT_BEGIN_UTC::timestamp_ntz)) as ActiveDates
      , sum(TOTAL_VIEWING_MINUTES) as TVM
      ,count (distinct SESSION_ID) as sessions
   from ODIN_PRD.RPT.ALL_HOURLY_TVS_AGG_US a
   where VIDEO_SEGMENT_BEGIN_UTC >= $start_date
        and VIDEO_SEGMENT_BEGIN_UTC <= $end_date
        and app_name in ($app_name)
        group by 1,2
)

-- get all clients (new users)
, new_users as
(
 select distinct CLIENT_ID,
     date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',CLIENT_FIRST_SEEN_UTC::timestamp_ntz)) as First_Seen_Dates
 from "ODIN_PRD"."RPT"."ALL_CLIENT_FIRST_SEEN"
  where CLIENT_FIRST_SEEN_UTC >= $start_date
    and CLIENT_FIRST_SEEN_UTC < $end_date
    and app_name = $app_name
)
-- get impressions
, impressions as
(    select CLIENT_ID, date_trunc('DAY', convert_timezone('UTC', 'America/Los_Angeles',HOUR_TIMESTAMP_SERVER::timestamp_ntz)) as impression_Dates, AD_SOURCE, count(CM_IMPRESSION_COUNT) as Impressions_Count
    from "HIST"."RPT"."HOURLY_CM_REALTIME_EVENT" 
        where HOUR_TIMESTAMP_SERVER >= $start_date
        and HOUR_TIMESTAMP_SERVER < $end_date
    group by 1,2,3
)        
-- join all the above tables (active users/KPIs in experiment who are new)
, opt_sessions as
(
    select agg.*, VARIATION_ID, AD_SOURCE, Impressions_Count
    from tvmtable agg
    inner join optimizely opt2
        on sha2(lower(agg.CLIENT_ID), 256) = opt2.visitor_id
    inner join new_users nu
        on agg.CLIENT_ID = nu.CLIENT_ID
        and agg.ActiveDates = nu.First_Seen_Dates
    inner join impressions imps
        on lower(agg.CLIENT_ID) = lower(imps.CLIENT_ID)
        and agg.ActiveDates = imps.impression_Dates
)
select ActiveDates,
    VARIATION_ID,
    AD_SOURCE,
    sum(Impressions_Count) as No_Impressions
from opt_sessions a
group by 1,2,3