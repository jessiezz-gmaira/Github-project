-- insert into ODIN_PRD.RPT.BI_USER_ENGAGEMENT360
    with et_table1 AS
            (
            SELECT
                CEF.CLIENT_GEO_SID,
                CEF.EVENT_OCCURRED_UTC,
                CEF.APP_SID,
                CEF.APP_VERSION,  
                CEF.CHANNEL_SID,  
                CEF.EVENT_NAME_SID,
                CEF.CONTENT_SESSION_SID,
                CEF.EVENT_EMITTER_IP_SID,
                CEF.USER_BEHAVIOR_SID,  
                CEF.USER_SID,
                ITEM_POSITION_X,
                ITEM_POSITION_Y,
                LABEL,
                PLAYBACK_SID,
                USER_INTERACTION_SID,
                UTM_SID
            FROM ODIN_PRD.DW_ODIN.CLIENT_EVENT_FACT AS CEF  
            WHERE cef.event_occurred_date_sid = '20220315' -- running 11/28 need 29 next | but working backward need oct 6
              --cef.event_occurred_date_sid = ${eventOccurredDateSid}
              and EVENT_NAME_SID IN 
              (SELECT DISTINCT EVENT_NAME_SID FROM ODIN_PRD.DW_ODIN.EVENT_NAME_DIM WHERE EVENT_NAME IN 
              ('episodeStart','appBackgrounded','appForegrounded','appLaunch','castRequest','castSuccess',
              'changePlaybackState','channelGuideLoaded','channelGuideRequest','cmError',
              'cmImpression','impressionNonAd','pageView','policyAccepted','policyViewed',
              'policyViewedAccepted','sessionReset'',''signInSuccessful','signOutSuccessful',
              'signUpSuccessful','undefinedError','userAction','videoError', 'clipStart', 'cmPodBegin'))
            )
            SELECT
            t3.EVENT_OCCURRED_UTC as TIME_STAMP,
            CLIENT.CLIENT_ID,
            CLIENT.client_model_name,
            CLIENT.client_model_number,
            CSB.SESSION_ID,
            t3.EVENT_EMITTER_IP_SID,
            APP_NAME,
            t3.APP_VERSION,
            t3.USER_SID,
            case when user_Sid = '-2009842' then 0 ELSE 1 end as IND_REGISTERED_USER, 
            geo.COUNTRY,
            case when geo.country='US' then 'domestic' else 'intl' end as REGION,
            seri.series_name, --this needs to be mapped from the cms_episode_dim to the cms_series_dim
            seri.series_id,
            chan.channel_name,
            chan.channel_id,
            epis.episode_name,
            epis.episode_id,
            clip.clip_name,
            clip.clip_id,
            EVENT_NAME,
            SCREEN_NAME,
            SCREEN_ELEMENT_NAME,
            ser.series_name as series_name_ub,
            cha.channel_name as channel_name_ub,
            epi.episode_name as episode_name_ub,
            vcd.name as vod_category_ub,
            ITEM_POSITION_X,
            ITEM_POSITION_Y,
            UB.position_id,
            LABEL,
            PLAYBACK_STATE,
            USER_INTERACTION_MODE,
            UTM_CAMPAIGN, 
            UTM_CONTENT, 
            UTM_MEDIUM, 
            UTM_SOURCE, 
            UTM_TERM
            FROM et_table1 t3
            JOIN ODIN_PRD.DW_ODIN.CLIENT_GEO_BRIDGE AS CGB ON t3.CLIENT_GEO_SID = CGB.CLIENT_GEO_SID
            JOIN ODIN_PRD.DW_ODIN.GEO_DIM  AS GEO ON CGB.GEO_SID = GEO.GEO_SID
            JOIN ODIN_PRD.DW_ODIN.CLIENT_DIM AS CLIENT ON CGB.CLIENT_SID = CLIENT.CLIENT_SID
            JOIN ODIN_PRD.DW_ODIN.CONTENT_SESSION_BRIDGE AS CSB ON t3.CONTENT_SESSION_SID = CSB.CONTENT_SESSION_SID
            left join ODIN_PRD.DW_ODIN.CMS_SERIES_DIM seri on csb.series_sid=seri.cms_series_sid
            left join ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM chan on t3.channel_sid=chan.cms_channel_sid
            left join ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM epis on csb.episode_sid=epis.cms_episode_sid  
            left join ODIN_PRD.DW_ODIN.CMS_CLIP_DIM clip on csb.clip_sid=clip.cms_clip_sid
            join ODIN_PRD.dw_odin.user_behavior_bridge ub on t3.user_behavior_sid=ub.user_behavior_sid
            join ODIN_PRD.dw_odin.app_dim app on t3.app_sid = app.app_sid
            left join ODIN_PRD.dw_odin.screen_dim sc on ub.screen_sid=sc.screen_sid
            left join ODIN_PRD.dw_odin.screen_element_dim se on ub.screen_element_sid=se.screen_element_sid
            left join ODIN_PRD.DW_ODIN.EVENT_NAME_DIM as event on t3.EVENT_NAME_SID = event.EVENT_NAME_SID
            left JOIN ODIN_PRD.DW_ODIN.PLAYBACK_DIM AS PLAY ON T3.PLAYBACK_SID = PLAY.PLAYBACK_SID
            left JOIN ODIN_PRD.DW_ODIN.INTERACTION_DIM AS INTR ON T3.USER_INTERACTION_SID = INTR.INTERACTION_SID
            left JOIN ODIN_PRD.DW_ODIN.UTM_DIM AS UTM ON T3.UTM_SID = UTM.UTM_SID
            left join ODIN_PRD.DW_ODIN.CMS_SERIES_DIM ser on ub.series_sid=ser.cms_series_sid
            left join ODIN_PRD.DW_ODIN.CMS_CHANNEL_DIM cha on ub.channel_sid=cha.cms_channel_sid
            left join ODIN_PRD.DW_ODIN.CMS_EPISODE_DIM epi on ub.episode_sid=epi.cms_episode_sid
            left join ODIN_PRD.DW_ODIN.CMS_VODCATEGORIES_DIM vcd on ub.vod_category_sid=vcd.cms_vodcategories_sid
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38