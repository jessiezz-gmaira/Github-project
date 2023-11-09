CREATE OR REPLACE TRANSIENT TABLE utm_cfeed as 
      select
        distinct month
        , app_name
        , client_sid
        , session_id
        , case when lower(country) = 'us' then 'US' else 'INTL' end as region
        , CASE WHEN channel_id = 'vod' then 'VOD' ELSE 'LINEAR' END AS content_type
        , utm_medium
        , utm_source
        , CASE WHEN utm_source IN ('comcast') AND utm_medium IN ('ossearch') OR 
         utm_source IN ('google') AND utm_medium IN ('livetab') OR 
         utm_source IN ('tivo') AND utm_medium IN ('deviceguidesearch') OR 
         utm_source IN ('hisense') AND utm_medium IN ('deviceguidesearch') OR 
         utm_source IN ('tclchannel') AND utm_medium IN ('deviceguidesearch') OR 
         utm_source IN ('verizon') AND utm_medium IN ('deviceguidesearch') OR 
         utm_source IN ('google') AND utm_medium IN ('ossearch', 'textsearch') OR
         utm_source IN ('samsung') AND utm_medium IN ('ossearch') OR 
         utm_source IN ('tivo') AND utm_medium IN ('ossearch') OR 
         utm_source IN ('stbverizon') AND utm_medium IN ('ossearch') OR 
         utm_source IN ('vizio') AND utm_medium IN ('ossearch') OR 
         utm_source IN ('apple') AND utm_medium IN ('ossearch') OR 
         utm_source IN ('hisense') AND utm_medium IN ('ossearch') OR 
         utm_source IN ('justwatch') AND utm_medium IN ('deeplink') OR
         utm_source IN ('reelgood') AND utm_medium IN ('deeplink') OR 
         utm_source IN ('tclchannel') AND utm_medium IN ('ossearch','contenttile') OR 
         utm_source IN ('lgwebos') AND utm_medium IN ('ossearch') THEN 1 ELSE 0
        END AS CFEED_FLAG 
        , sum(tvm) as tvm

        from (
            select
                distinct a.client_sid
                , a.session_id
                , date_trunc('month', h.utc) as month
                , app.app_name
                , u.utm_source
                , u.utm_medium
                , c.channel_id
                , g.country
                , sum(a.total_viewing_seconds/60) as tvm
                from ODIN_PRD.DW_ODIN.UTM_DIM u
                join ODIN_PRD.RPT.HOURLY_TVS_AGG a on u.utm_sid = a.utm_sid
                join ODIN_PRD.DW_ODIN.HOUR_DIM h on h.hour_sid = a.hour_sid
                join ODIN_PRD.DW_ODIN.APP_DIM app on app.app_sid = a.app_sid
                join ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM c on c.cms_channel_sid = a.cms_channel_sid
                join ODIN_PRD.DW_ODIN.GEO_DIM g ON g.GEO_SID = a.GEO_SID
                where date_trunc('month', h.utc) between '2023-01-01' and '2023-03-01'
                and app.app_name NOT IN ('web')
                and u.utm_medium IN ('ossearch', 'textsearch', 'liveteab', 'deeplink', 'deviceguidesearch', 'contenttile')
                and u.utm_source IN ('comcast','google','tivo','hisense','tclchannel','verizon','samsung','vizio','apple','hisense','justwatch','reelgood','lgwebos')
                group by 1,2,3,4,5,6,7,8
          
          union

            select
                distinct a.client_sid
                , a.session_id
                , date_trunc('month', h.utc) as month
                , app.app_name
                , u.utm_source
                , u.utm_medium
                , c.channel_id
                , g.country
                , sum(a.total_viewing_seconds/60) as tvm
                from ODIN_PRD.DW_ODIN.S_UTM_DIM u
                join ODIN_PRD.RPT.S_HOURLY_INACTIVE_TVS_AGG a on u.utm_sid = a.utm_sid
                join ODIN_PRD.DW_ODIN.HOUR_DIM h on h.hour_sid = a.hour_sid
                join ODIN_PRD.DW_ODIN.S_APP_DIM app on app.app_sid = a.app_sid
                join ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM c on c.cms_channel_sid = a.cms_channel_sid
                join ODIN_PRD.DW_ODIN.S_GEO_DIM g ON g.GEO_SID = a.GEO_SID
                where date_trunc('month', h.utc) between '2023-01-01' and '2023-03-01'
                and u.utm_medium IN ('ossearch', 'textsearch', 'liveteab', 'deeplink', 'deviceguidesearch', 'contenttile')
                and u.utm_source IN ('comcast','google','tivo','hisense','tclchannel','verizon','samsung','vizio','apple','hisense','justwatch','reelgood','lgwebos')
                group by 1,2,3,4,5,6,7,8
            )
    
        group by 1,2,3,4,5,6,7,8,9
;


    
SELECT month
, region
--, app_name
, content_type
, count(distinct client_sid) as mau
, sum(tvm) as tvms
FROM utm_cfeed
where cfeed_flag = 1
group by 1,2,3
order by 1 asc, 4 desc
;






/*

--app investigation
SELECT 
month
,app_name
,count(distinct client_sid) as mau
FROM utm_cfeed
group by 1,2
order by 1 asc, 3 desc
;

-- investigaing firetv
SELECT month
,utm_source
,utm_medium
,count(distinct client_sid) mau
,count(distinct session_id) sessions
FROM utm_cfeed
WHERE cfeed_flag = 1
and app_name = 'firetv'
--and utm_source = 'google'
--and utm_medium = 'textsearch'
group by 1,2,3
;





    , CASE WHEN utm_source IN ('comcast') AND utm_medium IN ('ossearch') then 'Comcast Linear'
        WHEN utm_source IN ('google') AND utm_medium IN ('livetab') then 'Google Refactored'
        WHEN utm_source IN ('tivo') AND utm_medium IN ('deviceguidesearch') then 'Tivo Linear'
        WHEN utm_source IN ('hisense') AND utm_medium IN ('deviceguidesearch') then 'Hisense VIDAA Linear'
        WHEN utm_source IN ('tclchannel') AND utm_medium IN ('deviceguidesearch') then 'TCL Channel'
        WHEN utm_source IN ('verizon') AND utm_medium IN ('deviceguidesearch') then 'Verizon'
        WHEN utm_source IN ('google') AND utm_medium IN ('ossearch', 'textsearch') then 'Google VOD'
        WHEN utm_source IN ('samsung') AND utm_medium IN ('ossearch') then 'Samsung Tizen VOD'
        WHEN utm_source IN ('tivo') AND utm_medium IN ('ossearch') then 'Tivo VOD'
        WHEN utm_source IN ('stbverizon') AND utm_medium IN ('ossearch') then 'Verizon VOD'
        WHEN utm_source IN ('vizio') AND utm_medium IN ('ossearch') then 'Vizio Smartcast VOD'
        WHEN utm_source IN ('apple') AND utm_medium IN ('ossearch') then 'Apple VOD'
        WHEN utm_source IN ('hisense') AND utm_medium IN ('ossearch') then 'Hisense VOD'
        WHEN utm_source IN ('justwatch') AND utm_medium IN ('deeplink') then 'JustWatch'
        WHEN utm_source IN ('reelgood') AND utm_medium IN ('deeplink') then 'ReelGood'
        WHEN utm_source IN ('tclchannel') AND utm_medium IN ('ossearch','contenttile') then 'TCL Channel'
        WHEN utm_source IN ('lgwebos') AND utm_medium IN ('ossearch') then 'LGWebOS VOD'
        END AS CFEED
 */