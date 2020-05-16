
drop table if exists staging_videos;
drop table if exists staging_channels;
drop table if exists trendingvideos;
drop table if exists videos;
drop table if exists channels;
drop table if exists category;
drop table if exists country;
drop table if exists most_trending_days_video;
drop table if exists most_trending_channel;


CREATE TABLE if not exists staging_videos (
			video_id varchar(256),
			trending_date date,
			title varchar(4096),
			channel_title varchar(4096),
			publish_date date,
			category_id int4,
			publish_time varchar(256),
			tags varchar(max),
			views bigint,
			likes bigint,
			dislikes bigint,
			comment_count bigint,
			thumbnail_link varchar(256),
			comments_disabled boolean,
			ratings_disabled boolean,
			video_error_or_removed boolean,
			description varchar(max),
			category_name varchar(256),
			region varchar(256)
		);

CREATE TABLE if not exists staging_channels (
			category_id int4,
			category_name varchar(256),
			channel_id varchar(256),
			country varchar(256),
			description varchar(max),
			followers bigint,
			join_date date,
			location varchar(256),
			picture_url varchar(256),
			profile_url varchar(256),
			title varchar(4096),
			trailer_title varchar(4096),
			trailer_url varchar(256),
			videos bigint
		);

CREATE TABLE if not exists trendingvideos (
			trending_date date NOT NULL,
			video_id varchar(256) NOT NULL,
			channel_id varchar(256),
			category_id int4,
			country_id int4
		);

CREATE TABLE if not exists videos (
			video_id varchar(256) NOT NULL,
			title varchar(4096),
			publish_date date,
			views bigint,
			likes bigint,
			dislikes bigint,
			comment_count bigint,
			CONSTRAINT videos_pkey PRIMARY KEY (video_id)
		);

CREATE TABLE if not exists channels (
			channel_id varchar(256) NOT NULL,
			title varchar(4096),
			followers bigint,
			videos bigint,
			join_date date,
			profile_url varchar(256),
			CONSTRAINT channels_pkey PRIMARY KEY (channel_id)
		);

CREATE TABLE if not exists category (
			category_id int4 NOT NULL,
			category_name varchar(256),
			CONSTRAINT category_pkey PRIMARY KEY (category_id)
		);

CREATE TABLE if not exists country (
			country_id int4 NOT NULL,
			country varchar(256),
			CONSTRAINT country_pkey PRIMARY KEY (country_id)
		);

CREATE TABLE if not exists most_trending_days_video (
			title varchar(4096),
			publish_date date,
			trending_days int4,
			views bigint,
			likes bigint,
			comment_count bigint
		);

CREATE TABLE if not exists most_trending_channel (
			title varchar(4096),
			join_date date,
			profile_url varchar(256),
			followers bigint,
			videos bigint,
			trendingvideo_cnt int4
		);
