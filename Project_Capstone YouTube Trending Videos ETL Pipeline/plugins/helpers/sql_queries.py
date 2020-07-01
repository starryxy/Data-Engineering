class SqlQueries:

    insert_trendingvideos_table = ("""
        insert into trendingvideos
        SELECT videos.trending_date,
               videos.video_id,
               channels.channel_id,
               videos.category_id,
               videos.country_id
        FROM (
               SELECT *,
                    case
                        when region = 'United States' then 1
                        when region = 'Canada' then 2
                        when region = 'Great Britan' then 3
                        when region = 'France' then 4
                        when region = 'Germany' then 5
                        when region = 'Mexico' then 6
                        when region = 'Japan' then 7
                        when region = 'South Korea' then 8
                        when region = 'India' then 9
                        when region = 'Russia' then 10
                    end as country_id
               FROM staging_videos
               WHERE video_id is not null
                    AND channel_title is not null
           ) videos
        LEFT JOIN staging_channels channels
            ON videos.channel_title = channels.title;
    """)

    insert_videos_table = ("""
        insert into videos
        SELECT t1.video_id,
               t2.title,
               t2.publish_date,
               t2.views,
               t2.likes,
               t2.dislikes,
               t2.comment_count
        FROM (
                SELECT video_id,
                       max(trending_date) as max_date
                FROM staging_videos
                WHERE video_id is not null
                GROUP BY video_id
            ) t1
        JOIN staging_videos t2
            ON t1.video_id = t2.video_id
            AND t1.max_date = t2.trending_date;
    """)

    insert_channels_table = ("""
        insert into channels
        SELECT channel_id,
               title,
               followers,
               videos,
               join_date,
               profile_url
        FROM staging_channels
        WHERE channel_id is not null;
    """)

    insert_category_table = ("""
        insert into category
        SELECT distinct category_id,
               category_name
        FROM staging_videos
        WHERE category_id is not null;
    """)

    insert_country_table = ("""
        insert into country values
            (1, 'United States'),
            (2, 'Canada'),
            (3, 'Great Britan'),
            (4, 'France'),
            (5, 'Germany'),
            (6, 'Mexico'),
            (7, 'Japan'),
            (8, 'South Korea'),
            (9, 'India'),
            (10, 'Russia');
    """)

    most_trending_days_video = ("""
        insert into most_trending_days_video
        SELECT t2.title,
            t2.publish_date,
            t1.days,
            t2.views,
            t2.likes,
            t2.comment_count
        FROM (
                SELECT video_id,
                    COUNT(distinct trending_date) as days
                FROM trendingvideos
                GROUP BY video_id
            ) t1
        JOIN videos t2
            ON t1.video_id = t2.video_id
        ORDER BY t1.days DESC
        LIMIT 1;
    """)

    most_trending_channel = ("""
        insert into most_trending_channel
        SELECT t2.title,
               t2.join_date,
               t2.profile_url,
               t2.followers,
               t2.videos,
               t1.cnt
        FROM (
                SELECT channel_id,
                    COUNT(distinct video_id) as cnt
                FROM trendingvideos
                GROUP BY channel_id
            ) t1
        JOIN channels t2
            ON t1.channel_id = t2.channel_id
        ORDER BY t1.cnt DESC
        LIMIT 1;
    """)
