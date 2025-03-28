---------------- Base table with rank of no of shows viewed for each host -----------------

DROP TABLE IF EXISTS analytics_scratch.ashika_show_viewers_affinity1;
CREATE TABLE analytics_scratch.ashika_show_viewers_affinity1 AS
select start_at,
       (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_shows.start_at )::integer), dw_shows.start_at  )), 'YYYY-MM-DD')) AS show_start_week,
       dw_shows.show_id,
       creator_id,
       live_show_host_activated_at,
       show_host_activated_at,
       dw_shows.origin_domain,
       title,
       type,
       CASE WHEN dw_shows.type = 'silent' OR dw_shows.title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END AS Is_silent_show,
       unique_viewers,
       viewer_id,
       show_viewer_activated_at,
    CASE WHEN show_viewer_events.viewer_id != show_viewer_events.host_id
    and (host_follow_clicks > 0
    OR sent_show_comments_clicks > 0
    OR sent_show_reactions_clicks > 0
    OR show_listing_likes_clicks > 0
    OR show_bid_clicks > 0
    OR show_listing_detail_clicks > 0
    OR show_host_closet_clicks > 0
    OR  show_viewer_events.show_giveaways_entered_clicks   > 0
    OR total_watched_show_minutes >= 1)
    THEN 'Yes' ELSE 'No' END AS is_engaged_viewer,
    rank() over (partition by viewer_id,creator_id order by start_at ) as no_of_shows_viewed

from analytics.dw_shows
left join analytics.dw_shows_cs on dw_shows.show_id = dw_shows_cs.show_id
left join analytics.dw_show_viewer_events_cs as show_viewer_events on dw_shows.show_id = show_viewer_events.show_id
left join analytics.dw_users_cs on dw_users_cs.user_id = dw_shows.creator_id
where start_at IS NOT NULL AND live_show_host_activated_at is not null
AND origin_domain = 'us' AND (CASE WHEN dw_shows.type = 'silent' OR dw_shows.title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END) = 'No'
       group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
GRANT ALL ON analytics_scratch.ashika_show_viewers_affinity1 TO PUBLIC;

select * from analytics_scratch.ashika_show_viewers_affinity1  limit 100;


---------------------- Latest Host segment does this host belongs to as of Feb END -----------------------------------


DROP TABLE IF EXISTS analytics_scratch.ashika_show_host_latest_segment1;
CREATE TABLE analytics_scratch.ashika_show_host_latest_segment1 AS

select DISTINCT creator_id, coalesce(host_segments_gmv_end.user_segment_daily, 'Segment 1: No Sales')  AS host_segment
from analytics_scratch.ashika_show_viewers_affinity1 as a
LEFT JOIN analytics.dw_user_segments  AS host_segments_gmv_end ON
            a.creator_id = host_segments_gmv_end.id and
            host_segments_gmv_end.user_type = 'show_host' and
            (TO_CHAR(DATE_TRUNC('month', DATE('2025-02-28') ), 'YYYY-MM')) >= (TO_CHAR(DATE_TRUNC('month', host_segments_gmv_end.start_date ), 'YYYY-MM')) and
            (TO_CHAR(DATE_TRUNC('month', DATE('2025-02-28') ), 'YYYY-MM')) < (TO_CHAR(DATE_TRUNC('month', coalesce(host_segments_gmv_end.end_date, GETDATE()) ), 'YYYY-MM'))
GRANT ALL ON analytics_scratch.ashika_show_host_latest_segment1 TO PUBLIC;


--------------------------- host level - total shows viewed by each viewers (max of the no of shows viewed rank) --------------


DROP TABLE IF EXISTS analytics_scratch.ashika_show_host_max_show_view1;
CREATE TABLE analytics_scratch.ashika_show_host_max_show_view1 AS
SELECT creator_id,
       live_show_host_activated_at,
       viewer_id,
       MAX(no_of_shows_viewed) as max_no_of_shows_viewed
from analytics_scratch.ashika_show_viewers_affinity1
group by 1,2,3;
GRANT ALL ON analytics_scratch.ashika_show_host_max_show_view1 TO PUBLIC;


-------------------------- Total shows count and latest live show start date ----------------------



DROP TABLE IF EXISTS analytics_scratch.ashika_show_host_latest_show1;
CREATE TABLE analytics_scratch.ashika_show_host_latest_show1 AS
SELECT creator_id,
             max(start_at) as latest_live_show_start_at,
             count(distinct show_id) as count_live_shows

from analytics_scratch.ashika_show_viewers_affinity1
group by 1
GRANT ALL ON analytics_scratch.ashika_show_host_latest_show1 TO PUBLIC;

select * from analytics_scratch.ashika_show_host_latest_show1 limit 100;


-------------------------------------------------------------------------------------  Lifetime stats of the hosts -------------------------------------------------------------------------



--------------------------------- host level - viewer details for lifetime  ---------------------


DROP TABLE IF EXISTS analytics_scratch.ashika_live_show_viewers;
CREATE TABLE analytics_scratch.ashika_live_show_viewers AS
SELECT a.creator_id,
       live_show_host_activated_at,
       latest_live_show_start_at,
       count_live_shows,
       host_segment,
       COUNT(distinct viewer_id) as viewer_count,
       COUNT(CASE WHEN max_no_of_shows_viewed = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(CASE WHEN max_no_of_shows_viewed >5 THEN viewer_id END) as viewer_viewed_5th_plus_show_of_the_host,
       COUNT(CASE WHEN max_no_of_shows_viewed >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host
FROM  analytics_scratch.ashika_show_host_max_show_view1 as a
left join analytics_scratch.ashika_show_host_latest_show1 as b on a.creator_id = b.creator_id
left join analytics_scratch.ashika_show_host_latest_segment1 as h on h.creator_id = a.creator_id
group by 1,2,3,4,5 order by viewer_viewed_20th_plus_show_of_the_host desc;
GRANT ALL ON analytics_scratch.ashika_live_show_viewers TO PUBLIC;

select * from analytics_scratch.ashika_live_show_viewers order by viewer_viewed_20th_plus_show_of_the_host desc limit 100


--------------------- overall stat (Lifetime stats of the host ) ------------------------


DROP TABLE IF EXISTS analytics_scratch.ashika_host_overall_stat;
CREATE TABLE analytics_scratch.ashika_host_overall_stat AS

Select g.creator_id,
       count(g.show_id) as total_shows,
       count(case when g.is_silent_show = 'No' then g.show_id end) as total_live_shows,
       sum(coalesce(g.live_auctions,0)) as total_live_auctions,
       sum(coalesce(g.total_orders,0)) as total_orders,
       sum(coalesce(g.total_order_items,0)) as total_order_items,
       sum(coalesce(g.total_NZ_order_items,0)) as total_NZ_order_items,
       sum(coalesce(g.total_order_gmv,0)) as total_order_gmv

       from (select dw_shows.creator_id,
       dw_shows.show_id,
       CASE WHEN dw_shows.type = 'silent' OR dw_shows.title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END as is_silent_show,
       live_auctions,
       total_orders,
       total_order_items,
       total_NZ_order_items,
       total_order_gmv
from analytics.dw_shows

left join (select dw_shows.creator_id,
       dw_auctions_cs.show_id,
       count( distinct auction_id) as live_auctions
from analytics.dw_auctions_cs
    left join (select distinct show_id,creator_id,type,title from analytics.dw_shows )dw_shows ON dw_auctions_cs.show_id = dw_shows.show_id
where (CASE WHEN dw_shows.type = 'silent' OR dw_shows.title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END) = 'No'
group by 1,2) as a
    ON  a.creator_id = dw_shows.creator_id and a.show_id = dw_shows.show_id

left join (select  show_id,   count( distinct order_id) as total_orders,
    sum(coalesce(order_number_items,0)) as total_order_items,
    sum(coalesce(order_number_items,0)) - sum(coalesce(order_number_items_giveaway,0)) as total_NZ_order_items,
    sum(coalesce(order_gmv_usd *0.01,0)) as total_order_gmv
           from analytics.dw_orders where show_id is not null group by 1)  as b ON b.show_id = dw_shows.show_id
where dw_shows.start_at is not null
    ) g group by  1;
GRANT ALL ON analytics_scratch.ashika_host_overall_stat TO PUBLIC;

-------------------------------------------------------------------------------------  Feb 2025 stats of the hosts -------------------------------------------------------------------------


-------------- Feb 2025 - host level - viewer details -----------------

DROP TABLE IF EXISTS analytics_scratch.ashika_live_show_viewers_feb;
CREATE TABLE analytics_scratch.ashika_live_show_viewers_feb AS
SELECT a.creator_id,
       COUNT(distinct viewer_id) as unique_viewer,
       COUNT(DISTINCT CASE WHEN date(start_at)= date(show_viewer_activated_at) then viewer_id end) as new_viewer,
       COUNT(DISTINCT CASE WHEN is_engaged_viewer = 'Yes' then viewer_id end) as engaged_viewer

FROM  analytics_scratch.ashika_show_viewers_affinity_feb as a
group by 1 order by new_viewer desc;
GRANT ALL ON analytics_scratch.ashika_live_show_viewers_feb TO PUBLIC;

-------------- Feb 2025 - order and show details of the hosts --------------

   DROP TABLE IF EXISTS analytics_scratch.ashika_host_feb_stat;
CREATE TABLE analytics_scratch.ashika_host_feb_stat AS

Select g.creator_id,
       count(g.show_id) as total_shows,
       count(case when g.is_silent_show = 'No' then g.show_id end) as total_live_shows,
       sum(coalesce(g.live_auctions,0)) as total_live_auctions,
       sum(coalesce(g.total_orders,0)) as total_orders,
       sum(coalesce(g.total_order_items,0)) as total_order_items,
       sum(coalesce(g.total_NZ_order_items,0)) as total_NZ_order_items,
       sum(coalesce(g.total_order_gmv,0)) as total_order_gmv

       from (select dw_shows.creator_id,
       dw_shows.show_id,
       CASE WHEN dw_shows.type = 'silent' OR dw_shows.title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END as is_silent_show,
       live_auctions,
       total_orders,
       total_order_items,
       total_NZ_order_items,
       total_order_gmv
from analytics.dw_shows

left join (select dw_shows.creator_id,
       dw_auctions_cs.show_id,
       count( distinct auction_id) as live_auctions
from analytics.dw_auctions_cs
left join (select distinct show_id,creator_id,type,title from analytics.dw_shows )dw_shows ON dw_auctions_cs.show_id = dw_shows.show_id
where (CASE WHEN dw_shows.type = 'silent' OR dw_shows.title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END) = 'No'
group by 1,2) as a
    ON  a.creator_id = dw_shows.creator_id and a.show_id = dw_shows.show_id

left join (select  show_id,   count( distinct order_id) as total_orders,
    sum(coalesce(order_number_items,0)) as total_order_items,
    sum(coalesce(order_number_items,0)) - sum(coalesce(order_number_items_giveaway,0)) as total_NZ_order_items,
    sum(coalesce(order_gmv_usd *0.01,0)) as total_order_gmv
           from analytics.dw_orders where show_id is not null group by 1)  as b ON b.show_id = dw_shows.show_id
where (DATE(dw_shows.start_at) >= '2025-02-01' AND DATE(dw_shows.start_at) < '2025-03-01')
    ) g group by  1;
GRANT ALL ON analytics_scratch.ashika_host_feb_stat TO PUBLIC;



-------------- Feb 2025 - Host level non zero gmv buyers  ----------------

DROP TABLE IF EXISTS analytics_scratch.ashika_host_feb_buyers;
CREATE TABLE analytics_scratch.ashika_host_feb_buyers AS
select  creator_id,count(distinct case when total_NZ_order_items > 0 then buyer_id end) as total_NZ_buyer ,count( distinct order_id) as total_orders,
    sum(coalesce(order_number_items,0)) as total_order_items,
    sum(coalesce(total_NZ_order_items,0)) as total_NZ_order_items,
    sum(coalesce(order_gmv_usd *0.01,0)) as total_order_gmv from
    (select distinct dw_shows.creator_id,
                     dw_orders.show_id,
                     buyer_id,
                     order_id,
                     order_number_items,
                     order_gmv_usd,
                     order_number_items-order_number_items_giveaway as total_NZ_order_items
           from analytics.dw_orders
left join analytics.dw_shows on dw_shows.show_id = dw_orders.show_id
where dw_orders.show_id is not null and (DATE(dw_shows.start_at) >= '2025-02-01' AND DATE(dw_shows.start_at) < '2025-03-01'))
    group by 1
GRANT ALL ON analytics_scratch.ashika_host_feb_buyers TO PUBLIC;


-----------------------------------------------------  Final table joining the overall lifetime stats and Feb stats of the hosts  -------------------------------------



DROP TABLE IF EXISTS analytics_scratch.ashika_final_good_host_cal;
CREATE TABLE analytics_scratch.ashika_final_good_host_cal AS
select a.creator_id,
       f.username,
       DATE(a.live_show_host_activated_at) as live_show_host_activated_date ,
       DATE(a.latest_live_show_start_at) as recent_live_show_start_date,
       a.host_segment as host_segment_as_of_feb_end,
       a.viewer_count as total_lifetime_unique_viewers,
       a.new_viewer_to_the_host as new_viewer_to_the_host,
       a.viewer_viewed_5th_plus_show_of_the_host,
       a.viewer_viewed_20th_plus_show_of_the_host,
       b.total_shows as total_lifetime_shows,
       b.total_live_shows as total_lifetime_live_shows,
       b.total_live_auctions as total_lifetime_live_auctions,
       b.total_order_items as total_lifetime_order_items,
       b.total_nz_order_items as total_lifetime_nz_gmv_order_items,
       b.total_order_gmv as total_lifetime_order_gmv,
       e.unique_viewer,
       e.new_viewer,
       e.engaged_viewer,
       c.total_shows,
       c.total_live_shows,
       c.total_live_auctions,
       c.total_order_items,
       c.total_nz_order_items,
       d.total_nz_buyer,
       c.total_order_gmv

from analytics_scratch.ashika_live_show_viewers as a
left join analytics_scratch.ashika_host_overall_stat as b on a.creator_id = b.creator_id
left join analytics_scratch.ashika_live_show_viewers_feb as e on a.creator_id = e.creator_id
left join analytics_scratch.ashika_host_feb_stat as c on a.creator_id = c.creator_id
left join analytics_scratch.ashika_host_feb_buyers as d on a.creator_id = d.creator_id
left join analytics.dw_users_info  as f on a.creator_id = f.user_id
order by  viewer_viewed_5th_plus_show_of_the_host desc
GRANT ALL ON analytics_scratch.ashika_final_good_host_cal TO PUBLIC;


---------------------------------  List of hosts who have had at least 500 of Viewers who have watched their show more than 5 times in their lifetime. ----------------------------------------


select a.creator_id,
       f.username,
       DATE(a.live_show_host_activated_at) as live_show_host_activated_date ,
       DATE(a.latest_live_show_start_at) as recent_live_show_start_date,
       a.host_segment as host_segment_as_of_feb_end,
       a.viewer_count as total_lifetime_unique_viewers,
       a.new_viewer_to_the_host as new_viewer_to_the_host,
       a.viewer_viewed_5th_plus_show_of_the_host,
       b.total_shows as total_lifetime_shows,
       b.total_live_shows as total_lifetime_live_shows,
       b.total_live_auctions as total_lifetime_live_auctions,
       b.total_order_items as total_lifetime_order_items,
       b.total_nz_order_items as total_lifetime_nz_gmv_order_items,
       b.total_order_gmv as total_lifetime_order_gmv,
       e.unique_viewer,
       e.new_viewer,
       e.engaged_viewer,
       c.total_shows,
       c.total_live_shows,
       c.total_live_auctions,
       c.total_order_items,
       c.total_nz_order_items,
       d.total_nz_buyer,
       c.total_order_gmv

from analytics_scratch.ashika_live_show_viewers as a
left join analytics_scratch.ashika_host_overall_stat as b on a.creator_id = b.creator_id
left join analytics_scratch.ashika_live_show_viewers_feb as e on a.creator_id = e.creator_id
left join analytics_scratch.ashika_host_feb_stat as c on a.creator_id = c.creator_id
left join analytics_scratch.ashika_host_feb_buyers as d on a.creator_id = d.creator_id
left join analytics.dw_users_info  as f on a.creator_id = f.user_id
where viewer_viewed_5th_plus_show_of_the_host > 500
order by  viewer_viewed_5th_plus_show_of_the_host desc;





---------------------------------  List of hosts who have had at least 100 of Viewers who have watched their show more than 20 times in their lifetime. ----------------------------------------



select a.creator_id,
       f.username,
       DATE(a.live_show_host_activated_at) as live_show_host_activated_date ,
       DATE(a.latest_live_show_start_at) as recent_live_show_start_date,
       a.host_segment as host_segment_as_of_feb_end,
       a.viewer_count as total_lifetime_unique_viewers,
       a.new_viewer_to_the_host as new_viewer_to_the_host,
       a.viewer_viewed_20th_plus_show_of_the_host,
       b.total_shows as total_lifetime_shows,
       b.total_live_shows as total_lifetime_live_shows,
       b.total_live_auctions as total_lifetime_live_auctions,
       b.total_order_items as total_lifetime_order_items,
       b.total_nz_order_items as total_lifetime_nz_gmv_order_items,
       b.total_order_gmv as total_lifetime_order_gmv,
       e.unique_viewer,
       e.new_viewer,
       e.engaged_viewer,
       c.total_shows,
       c.total_live_shows,
       c.total_live_auctions,
       c.total_order_items,
       c.total_nz_order_items,
       d.total_nz_buyer,
       c.total_order_gmv

from analytics_scratch.ashika_live_show_viewers as a
left join analytics_scratch.ashika_host_overall_stat as b on a.creator_id = b.creator_id
left join analytics_scratch.ashika_live_show_viewers_feb as e on a.creator_id = e.creator_id
left join analytics_scratch.ashika_host_feb_stat as c on a.creator_id = c.creator_id
left join analytics_scratch.ashika_host_feb_buyers as d on a.creator_id = d.creator_id
left join analytics.dw_users_info  as f on a.creator_id = f.user_id
where viewer_viewed_20th_plus_show_of_the_host > 100
order by  viewer_viewed_20th_plus_show_of_the_host desc;






--------------------------  Hosts segment level distribution of hosts based on the no of shows thay have done on the month of Feb 2025 --------------- 


select host_segment_as_of_feb_end, CASE when total_shows is null THEN ' No Shows'
            when total_shows is not null AND total_live_shows = 0 THEN 'Done_Silent_Show_but_No_Live_Show'
            when total_shows is not null AND (total_live_shows > 0 AND total_live_shows <=5) THEN 'Done_1_to_5_shows'
            when total_shows is not null AND (total_live_shows > 5 AND total_live_shows <=20) THEN 'Done_5_to_20_shows'
            when total_shows is not null AND  total_live_shows > 20 THEN 'Done_more_than_20_shows'
                END AS no_of_live_shows_hosted_bucket,

       count(creator_id),
       count(case when viewer_viewed_5th_plus_show_of_the_host > 500  then creator_id end ) as viewed_5_plus,
       count(case when viewer_viewed_20th_plus_show_of_the_host > 100 then creator_id end ) as viewed_20_plus

from analytics_scratch.ashika_final_good_host_cal
group by 1,2
order by 1,2;


--------------------------  Hosts segment level distribution of hosts based on the no of shows and their order items and gmv for the month of Feb 2025 --------------- 

select host_segment_as_of_feb_end, CASE when total_shows is null THEN ' No Shows'
           when total_shows is not null AND total_live_shows = 0 THEN 'Done_Silent_Show_but_No_Live_Show'
            when total_shows is not null AND (total_live_shows > 0 AND total_live_shows <=5) THEN 'Done_1_to_5_shows'
            when total_shows is not null AND (total_live_shows > 5 AND total_live_shows <=20) THEN 'Done_5_to_20_shows'
            when total_shows is not null AND  total_live_shows > 20 THEN 'Done_more_than_20_shows'
                END AS no_of_live_shows_hosted_bucket,

       count(creator_id),
       sum(total_nz_order_items),
       sum(total_order_gmv)

from analytics_scratch.ashika_final_good_host_cal
where viewer_viewed_20th_plus_show_of_the_host > 100  -- viewer_viewed_5th_plus_show_of_the_host > 500
group by 1,2
order by 1,2;










