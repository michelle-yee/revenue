WITH single_pv AS 
(
SELECT
	DATE_TRUNC('month',CONVERT_TIMEZONE('PST8PDT',time)) as themonth, 
	COUNT(DISTINCT CASE WHEN path ILIKE '%refer-a-friend%'AND PATH <> '/refer-a-friend/' THEN user_id END) AS raf_inapp,
	COUNT(DISTINCT CASE WHEN path ILIKE '%friends-of-bench%' THEN user_id END) AS raf_lp,
	COUNT(DISTINCT CASE WHEN utm_medium IN ('display','cpc') THEN user_id END) AS paid_pv
FROM main_production.pageviews 
WHERE 
	(
	(PATH ILIKE '%refer-a-friend%'AND PATH <> '/refer-a-friend/') 
		OR PATH ILIKE '%friends-of-bench%'
		OR utm_medium IN ('display','cpc')
	)
	AND CONVERT_TIMEZONE('PST8PDT',time) >= '2020-05-01'
GROUP BY 1
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
	COUNT(DISTINCT CASE WHEN (ak_sup AND visited_blog) OR (visited_signup_page AND visited_blog) THEN user_id END) AS blog_sup,
	COUNT(DISTINCT CASE WHEN visited_blog AND visited_resource THEN user_id END) AS blog_resource
FROM session_views
GROUP BY themonth
),

raf_clicks AS --surfaces all clicks made on the in-app raf share link
(
SELECT
	DATE_TRUNC('month',CONVERT_TIMEZONE('PST8PDT',time)) AS themonth,
	COUNT(DISTINCT user_id) AS raf_clicks
FROM main_production.marketing_refer_a_friend_copy_click
WHERE CONVERT_TIMEZONE('PST8PDT',time) >= '2020-05-01'
GROUP BY 1
)
	
SELECT 
	NVL(multi_pv.themonth,single_pv.themonth) AS themonth,
	raf_inapp,
	raf_lp,
	raf_clicks,
	paid_pv,
	blog_sup,
	blog_resource
FROM multi_pv
	FULL OUTER JOIN single_pv ON multi_pv.themonth = single_pv.themonth
	LEFT JOIN raf_clicks ON multi_pv.themonth = raf_clicks.themonth;
