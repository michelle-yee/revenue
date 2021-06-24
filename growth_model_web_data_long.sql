WITH single_pv AS 
(
SELECT
	DATE_TRUNC('month',CONVERT_TIMEZONE('PST8PDT',time)) as themonth, 
	COUNT(DISTINCT user_id) as count,
	CASE WHEN path ILIKE '%refer-a-friend%'AND PATH <> '/refer-a-friend/' THEN 'raf_inapp_pv'
		WHEN path ILIKE '%friends-of-bench%' THEN 'raf_lp_pv'
		WHEN utm_medium IN ('display','cpc') THEN 'paid_pv'
	END as web_activity
FROM main_production.pageviews 
WHERE 
	(
	(PATH ILIKE '%refer-a-friend%'AND PATH <> '/refer-a-friend/') 
		OR PATH ILIKE '%friends-of-bench%'
		OR utm_medium IN ('display','cpc')
	)
	AND CONVERT_TIMEZONE('PST8PDT',time) >= '2020-05-01'
GROUP BY 1,3
),

session_views AS 
(
SELECT 
	DATE_TRUNC('month',CONVERT_TIMEZONE('PST8PDT',time)) as themonth,
	user_id,
	session_id,
	BOOL_OR(landing_page ILIKE '%join.bench%' AND landing_page NOT ILIKE '%expense-quiz%') AS ak_SUP,
	BOOL_OR(path ILIKE '%blog%'AND path NOT ILIKE '%/blog/resources%') AS visited_blog,  
	BOOL_OR(path ILIKE '%signup%') AS visited_signup_page,
	BOOL_OR(path ILIKE '%resources%') AS visited_resource 
FROM main_production.pageviews 
WHERE DATE_TRUNC('month',CONVERT_TIMEZONE('PST8PDT',time)) >= '2020-05-01'
GROUP BY 1,2,3
),

multi_pv AS --surfaces views from the blog to x within the same session
(
SELECT 
	themonth,
	COUNT(DISTINCT user_id) AS count,
	CASE WHEN (ak_sup AND visited_blog) OR (visited_signup_page AND visited_blog) THEN 'blog_sup' 
		WHEN visited_blog AND visited_resource THEN 'blog_resource'
	END AS web_activity
FROM session_views
WHERE web_activity IS NOT NULL --explicity filtering for blog to x 
GROUP BY 1,3
),

raf_clicks AS --surfaces all clicks made on the in-app raf share link
(
SELECT
	DATE_TRUNC('month',CONVERT_TIMEZONE('PST8PDT',time)) AS themonth,
	COUNT(DISTINCT user_id) AS count,
	'raf_clicks' AS web_activity
FROM main_production.marketing_refer_a_friend_copy_click
WHERE CONVERT_TIMEZONE('PST8PDT',time) >= '2020-05-01'
GROUP BY 1,3
)

SELECT
	themonth,
	count,
	web_activity
FROM single_pv

UNION

SELECT
	themonth,
	count,
	web_activity
FROM multi_pv

UNION

SELECT
	themonth,
	count,
	web_activity
FROM raf_clicks;
