with base as (        
select a.client_id, a.date, a.tenure, b.tenure, 
  case when round(tvm,0) < 5 then '0 - 0 to 5'
    when round(tvm,0) >= 5 and round(tvm,0) < 20 then '1 = 5 to 20'
    when round(tvm,0) >= 20 and round(tvm,0) < 120 then '2 = 20 to 120'
    when round(tvm,0) >= 120 and round(tvm,0) < 200 then '3 = 120 to 200'
    else 'over 200' end as tvm,
 sum(case when screen_element_name ='livel2nav' then 1 else 0 end) as livel2nav,
 sum(case when screen_element_name ='seechanneldetails2' then 1 else 0 end) as seechanneldetails2,  
 sum(case when screen_element_name ='seechanneldetails1' then 1 else 0 end) as seechanneldetails1,   
 sum(case when screen_element_name ='favoritechannel' then 1 else 0 end) as favoritechannel,    
 sum(case when screen_element_name ='vodl2nav' then 1 else 0 end) as vodl2nav,   
 sum(case when screen_element_name ='watchnow' then 1 else 0 end) as watchnow,   
 sum(case when screen_element_name ='addtowatchlist' then 1 else 0 end) as addtowatchlist,  
 sum(case when screen_element_name ='watchfromstart' then 1 else 0 end) as watchfromstart,  
 sum(case when screen_element_name ='search' then 1 else 0 end) as search,  
 sum(case when screen_element_name ='turnonkidsmode' then 1 else 0 end) as turnonkidsmode,  
 sum(case when screen_element_name ='signup' then 1 else 0 end) as signup,   
 sum(case when screen_element_name ='continuewatching' then 1 else 0 end) as continuewatching,   
 sum(case when screen_element_name ='herocarouseldetails' then 1 else 0 end) as herocarouseldetails,  
 sum(case when screen_element_name ='ptbseechanneldetails' then 1 else 0 end) as ptbseechanneldetails, 
 sum(case when screen_element_name ='idlepreferenceoff' then 1 else 0 end) as idlepreferenceoff,   
 sum(case when screen_element_name ='seevodseriesdetails' then 1 else 0 end) as seevodseriesdetails, 
 sum(case when screen_element_name ='seevodmoviedetails' then 1 else 0 end) as seevodmoviedetails, 
 sum(case when screen_element_name ='searchresulttile' then 1 else 0 end) as searchresulttile, 
 sum(case when screen_element_name ='seevodcollection' then 1 else 0 end) as seevodcollection, 
 sum(case when screen_element_name ='seevodepisode' then 1 else 0 end) as seevodepisode
from ROKU_NEWUSERSAUG_GM_100722 a
JOIN ROKU_NEWUSERSAUG_M1_TVM_GM_101322 b 
on a.client_id = b.client_id
    and a.tenure = b.tenure
left join ODIN_PRD.RPT.BI_USER_ENGAGEMENT360 c on a.client_id = c.client_id    
    where a.tenure = 0 --and a.client_id in ('fd5d6d74-272e-53da-a79a-ab214ff1860f', 'dad74056-2474-56c9-97e0-cbfb75224a4c')
group by 1,2,3,4,5--,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
)
, rtn as (
    SELECT client_id
    FROM ROKU_NEWUSERSAUG_GM_100722
    where tenure between 1 and 30
    group by 1
    order by 1)
, combine as (
    select a.client_id, date, 
    case when b.client_id is null then 0 else 1 END rtn, 
case when livel2nav >= 1 then 1 else 0 end as livel2nav,
case when seechanneldetails1 >= 1 then 1 else 0 end as seechanneldetails1,
case when seechanneldetails2 >= 1 then 1 else 0 end as seechanneldetails2,
case when favoritechannel >= 1 then 1 else 0 end as favoritechannel,
case when vodl2nav >= 1 then 1 else 0 end as vodl2nav,
case when watchnow >= 1 then 1 else 0 end as watchnow,
case when addtowatchlist >= 1 then 1 else 0 end as addtowatchlist,
case when watchfromstart >= 1 then 1 else 0 end as watchfromstart,
case when search >= 1 then 1 else 0 end as search,
case when turnonkidsmode >= 1 then 1 else 0 end as turnonkidsmode,
case when signup >= 1 then 1 else 0 end as signup,
case when continuewatching >= 1 then 1 else 0 end as continuewatching,
case when herocarouseldetails >= 1 then 1 else 0 end as herocarouseldetails,
case when ptbseechanneldetails >= 1 then 1 else 0 end as ptbseechanneldetails,
case when idlepreferenceoff >= 1 then 1 else 0 end as idlepreferenceoff,
case when seevodseriesdetails >= 1 then 1 else 0 end as seevodseriesdetails,  
case when seevodmoviedetails >= 1 then 1 else 0 end as seevodmoviedetails,  
case when searchresulttile >= 1 then 1 else 0 end as searchresulttile,
case when seevodcollection >= 1 then 1 else 0 end as seevodcollection,
case when seevodepisode >= 1 then 1 else 0 end as seevodepisode,
       TVM
    from base a left join rtn b on a.client_id =b.client_id
    )

SELECT 

rtn, livel2nav, seechanneldetails1, seechanneldetails2,
favoritechannel, vodl2nav, watchnow, addtowatchlist, watchfromstart,
search, turnonkidsmode, signup, continuewatching, herocarouseldetails,
ptbseechanneldetails, idlepreferenceoff, seevodseriesdetails, seevodmoviedetails,  
searchresulttile, seevodcollection, seevodepisode,tvm,
count(distinct client_id) as users

FROM combine
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
order by 22,1;