--- Base table for Host Viewer Affinity

DROP TABLE IF EXISTS analytics_scratch.ashika_show_host_viewers_retention;
CREATE TABLE analytics_scratch.ashika_show_host_viewers_retention AS
select start_at,
       (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_shows.start_at )::integer), dw_shows.start_at  )), 'YYYY-MM-DD')) AS show_start_week,
       dw_shows.show_id,
       creator_id,
       dw_shows.origin_domain,
       title,
       type,
       CASE WHEN dw_shows.type = 'silent' OR dw_shows.title ILIKE '%silent%' 
            THEN 'Yes' 
            ELSE 'No' 
       END AS Is_silent_show,
  
       unique_viewers,
       viewer_id,
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
            THEN 'Yes' 
            ELSE 'No' 
       END                                                                                     AS is_engaged_viewer,
       CASE WHEN follower_id  IS NOT NULL 
            THEN 'Yes' 
            ELSE 'No' 
       END                                                                                     AS is_a_follower,
       CASE WHEN a.show_id IS NOT NULL 
            THEN 'Yes'  
            ELSE  'No' 
       END                                                                                     AS is_giveaway_show,
       CASE WHEN order_id is not null 
            then 'yes' 
            else 'no' 
       end                                                                                     as is_ordered,
       coalesce(host_segments_gmv_start.user_segment_daily, 'Segment 1: No Sales')             AS host_segment,
       rank() over (partition by viewer_id,creator_id order by show_start_week )               as no_of_shows_viewed,

       count(distinct order_id)                                                                as total_orders,
       sum(coalesce(order_number_items,0))                                                     as total_order_items,
       sum(coalesce(order_gmv_usd *0.01,0))                                                    as total_order_gmv
  from analytics.dw_shows
    left join analytics.dw_shows_cs on dw_shows.show_id = dw_shows_cs.show_id
  
    LEFT JOIN analytics.dw_user_segments  AS host_segments_gmv_start
          ON dw_shows.creator_id = host_segments_gmv_start.id and 
             host_segments_gmv_start.user_type = 'show_host' and
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_shows.start_at )::integer), dw_shows.start_at  )), 'YYYY-MM-DD')) >
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM host_segments_gmv_start.start_date )::integer), host_segments_gmv_start.start_date  )), 'YYYY-MM-DD')) and  
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_shows.start_at )::integer), dw_shows.start_at  )), 'YYYY-MM-DD')) <=
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM coalesce(host_segments_gmv_start.end_date, GETDATE()) )::integer), coalesce(host_segments_gmv_start.end_date, GETDATE())  )), 'YYYY-MM-DD'))
  
   left join analytics.dw_show_viewer_events_cs as show_viewer_events on dw_shows.show_id = show_viewer_events.show_id
   left join analytics.dw_host_follow_history_quarterly on dw_shows.creator_id = dw_host_follow_history_quarterly.host_id
            AND dw_host_follow_history_quarterly.follower_id = show_viewer_events.viewer_id
   left join (select distinct show_id from analytics.dw_giveaways) as a On a.show_id = dw_shows.show_id
   left join  analytics.dw_orders ON dw_orders.show_id = dw_shows.show_id AND buyer_id = viewer_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

GRANT ALL ON analytics_scratch.ashika_show_host_viewers_retention TO PUBLIC;




----- Distinct viewers in each show host viewer bucket (segment) and these viewers wil not be repeated in other buckets. A viewer is categorized into different buckets based on the max number of shows they have watched from any host.
/* if they have watched 20 or more shows from any host, they belong to the "viewed_20th_+_show_of_the_host" bucket; 
if they have watched a maximum of 6 shows from any host, they fall into the "5-6_shows" bucket; and 
if they have watched only one show from any host and no more, they are classified as a "new_host_viewer. */


Select show_start_week,
       origin_domain,
       CASE WHEN max_no_shows_viewed_for_a_host = 1                                           THEN 'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5     THEN 'viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20    THEN 'viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20                                           THEN 'viewed_20th_+_show_of_the_host'
        END                                                                                     as host_viewer_bucket,
  
    count(distinct viewer_id) as count_unique_viewer,
    count(distinct CASE WHEN count_engaged_views>0 then viewer_id end)                          as count_unique_engaged_viewer

from (select show_start_week,
             origin_domain,
             viewer_id,
             sum(CASE WHEN is_engaged_viewer ='Yes' THEN 1 else 0 end)     as count_engaged_views,
             max(no_of_shows_viewed)                                       as max_no_shows_viewed_for_a_host

        from analytics_scratch.ashika_show_viewers_retention
            where show_start_week >= '2024-09-01'
            group by 1,2,3)
group by 1,2,3
order by 1 desc,2,3,4;


-- The Unique viewers in each host viewer bucket. The viewer is categorised into   different buckets based on the number of shows they have watched from any host. 
-- These viewers a unique in each segment but, can come under the other segments as well.




select show_start_week,
       origin_domain,
       Is_silent_show,
       is_engaged_viewer,
       is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as is_engaged_viewer,
       is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv
from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       is_engaged_viewer,
       'All' as is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       is_engaged_viewer,
       is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as is_engaged_viewer,
       'All' as is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       is_engaged_viewer,
       'All' as is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as is_engaged_viewer,
       is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as is_engaged_viewer,
       'All' as is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
order by 1 desc,2,3,4,5,6;


--- Similar to the above , added a host segment field into the code 



select show_start_week,
       origin_domain,
       Is_silent_show,
       host_segment,
       is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as host_segment,
       is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv
from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       host_segment,
       'All' as is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       host_segment,
       is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as host_segment,
       'All' as is_giveaway_show,
       CASE WHEN no_of_shows_viewed = 1 THEN 'new_viewer_to_host'
            WHEN no_of_shows_viewed >1 AND no_of_shows_viewed <=5 THEN 'viewed_atleast_5_shows_of_theirs'
            WHEN no_of_shows_viewed >5 AND no_of_shows_viewed <=20 THEN 'viewed_atleast_5_to_20_shows_of_theirs'
            WHEN no_of_shows_viewed >20 THEN 'viewed_more_than_20_shows_of_their'
        END as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       host_segment,
       'All' as is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as host_segment,
       is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
UNION ALL
select show_start_week,
       origin_domain,
       Is_silent_show,
       'All' as host_segment,
       'All' as is_giveaway_show,
       'All' as repeat_host_viewer_bucket,
       count(distinct show_id) as total_shows,
       count(distinct creator_id) as total_hosts,
       count(distinct viewer_id) as total_unique_viewers,
       count(distinct viewer_id||show_id) as total_show_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id end) as total_engaged_unique_viewers,
       count(distinct case when is_engaged_viewer='Yes' then viewer_id|| show_id end) as total_engaged_show_viewers,
       count(distinct case when is_ordered='yes' then viewer_id end) as total_unique_buyers,
       count(distinct case when is_ordered='yes' then viewer_id||show_id end) as total_show_buyers,
       sum(coalesce(total_orders,0)) AS total_orders,
       sum(coalesce(total_order_items,0)) AS total_order_items,
       sum(coalesce(total_order_gmv,0)) AS total_order_gmv

from analytics_scratch.ashika_show_host_viewers_retention
where show_start_week >= '2024-09-01'
group by 1,2,3,4,5,6
order by 1 desc,2,3,4,5,6;







