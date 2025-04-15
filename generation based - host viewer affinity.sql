
--- For Generation based host affinity ( monthly level )

DROP TABLE IF EXISTS analytics_scratch.ashika_show_host_viewers_retention1;
CREATE TABLE analytics_scratch.ashika_show_host_viewers_retention1 AS
select start_at,
       (TO_CHAR(DATE_TRUNC('month', dw_shows.start_at ), 'YYYY-MM')) AS show_start_month,
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
       rank() over (partition by viewer_id,creator_id order by start_at )               as no_of_shows_viewed,

       count(distinct order_id)                                                                as total_orders,
       sum(coalesce(order_number_items,0))                                                     as total_order_items,
       sum(coalesce(order_number_items_giveaway,0))                                            as total_order_number_items_giveaway,
       sum(coalesce(order_gmv_usd *0.01,0))                                                    as total_order_gmv
  from analytics.dw_shows
    left join analytics.dw_shows_cs on dw_shows.show_id = dw_shows_cs.show_id

    LEFT JOIN analytics.dw_user_segments  AS host_segments_gmv_start
          ON dw_shows.creator_id = host_segments_gmv_start.id and
             host_segments_gmv_start.user_type = 'show_host' and
             (TO_CHAR(DATE_TRUNC('month', dw_shows.start_at ), 'YYYY-MM')) > (TO_CHAR(DATE_TRUNC('month', host_segments_gmv_start.start_date ), 'YYYY-MM')) and
             (TO_CHAR(DATE_TRUNC('month', dw_shows.start_at ), 'YYYY-MM')) <= (TO_CHAR(DATE_TRUNC('month', coalesce(host_segments_gmv_start.end_date, GETDATE()) ), 'YYYY-MM'))

   left join analytics.dw_show_viewer_events_cs as show_viewer_events on dw_shows.show_id = show_viewer_events.show_id
   left join analytics.dw_host_follow_history_quarterly on dw_shows.creator_id = dw_host_follow_history_quarterly.host_id
            AND dw_host_follow_history_quarterly.follower_id = show_viewer_events.viewer_id
   left join (select distinct show_id from analytics.dw_giveaways) as a On a.show_id = dw_shows.show_id
   left join  analytics.dw_orders ON dw_orders.show_id = dw_shows.show_id AND buyer_id = viewer_id
  where ((( dw_shows.start_at  ) >= ((DATEADD(month,-8, DATE_TRUNC('month', DATE_TRUNC('day',GETDATE())) ))) AND ( dw_shows.start_at  )
        < ((DATEADD(month,8, DATEADD(month,-8, DATE_TRUNC('month', DATE_TRUNC('day',GETDATE())) ) )))))
  AND viewer_id is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;



DROP TABLE IF EXISTS analytics_scratch.ashika_show_host_viewers_aff;
CREATE TABLE analytics_scratch.ashika_show_host_viewers_aff AS
select show_start_month,
        origin_domain,
            creator_id,
             viewer_id,
             max(no_of_shows_viewed)                                       as max_no_shows_viewed_for_a_host

        from analytics_scratch.ashika_show_host_viewers_retention1
            group by 1,2,3,4

GRANT ALL ON analytics_scratch.ashika_show_host_viewers_aff TO PUBLIC;



DROP TABLE IF EXISTS analytics_scratch.ashika_show_host_viewers_with_generation;
CREATE TABLE analytics_scratch.ashika_show_host_viewers_with_generation AS
SELECT b.generation as host_generation,
       coalesce(c.generation, 'unknown')   as viewer_generation,
       d.max_no_shows_viewed_for_a_host,
       a.*
        FROM analytics_scratch.ashika_show_host_viewers_retention1 as a
             left join analytics.dw_users  as b on a.creator_id = b.user_id
             left join analytics.dw_users  as c on a.viewer_id = c.user_id
             left join analytics_scratch.ashika_show_host_viewers_aff as d
             on a.show_start_month = d.show_start_month AND a.origin_domain = d.origin_domain
             AND a.creator_id = d.creator_id AND a.viewer_id = d.viewer_id
GRANT ALL ON analytics_scratch.ashika_show_host_viewers_with_generation TO PUBLIC;



---- with generation the hosts and shows

SELECT show_start_month,
       Is_silent_show,
       host_segment,
       host_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv) as order_gmv
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       'All' as host_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
        host_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       'All' as host_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4
order by 1 desc , 2,3,4



SELECT show_start_month,
        Is_silent_show,
       host_segment,
       host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       'All' as host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       'All' as host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       'All' as host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       'All' as host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5

order by 1 desc , 2,3,4,5


----- with max views


SELECT show_start_month,
        Is_silent_show,
       host_segment,
       host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       'All' as host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,

       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT( distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       'All' as host_generation,
       viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       'All' as host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       'All' as host_generation,
       'All' as viewer_generation,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN viewer_id END) as new_viewer_to_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN viewer_id END) as viewer_viewed_the_2nd_to_5th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN viewer_id END) as viewer_viewed_the_6th_to_20th_show_of_the_host,
       COUNT(distinct CASE WHEN max_no_shows_viewed_for_a_host >20 THEN viewer_id END) as viewer_viewed_20th_plus_show_of_the_host,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5

order by 1 desc , 2,3,4,5

--- the view as a segment


SELECT show_start_month,
        Is_silent_show,
       host_segment,
       host_generation,
       viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       host_generation,
       'All' as viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       'All' as host_generation,
       viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       host_generation,
       viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       'All' as host_generation,
       viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       host_generation,
       'All' as viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       host_segment,
       'All' as host_generation,
       'All' as viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6
UNION ALL
SELECT show_start_month,
        Is_silent_show,
       'All' as host_segment,
       'All' as host_generation,
       'All' as viewer_generation,
       CASE WHEN max_no_shows_viewed_for_a_host = 1 THEN  'new_viewer_to_the_host'
            WHEN max_no_shows_viewed_for_a_host >1 AND max_no_shows_viewed_for_a_host <=5 THEN  'viewer_viewed_the_2nd_to_5th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >5 AND max_no_shows_viewed_for_a_host <=20 THEN  'viewer_viewed_the_6th_to_20th_show_of_the_host'
            WHEN max_no_shows_viewed_for_a_host >20 THEN  'viewer_viewed_20th_plus_show_of_the_host'
            END AS viewer_segment,
       count(distinct creator_id) as count_host,
       count( distinct show_id) as count_shows,
       count( distinct viewer_id) as count_unique_viewers,
       count(distinct case when is_engaged_viewer = 'Yes' Then viewer_id END) as count_engaged_viewers,
       sum(total_order_items) - sum(total_order_number_items_giveaway) as nz_order_items,
       sum(total_order_gmv)
from analytics_scratch.ashika_show_host_viewers_with_generation
where origin_domain ='us'
group by 1,2,3,4,5,6

order by 1 desc , 2,3,4,5,6
