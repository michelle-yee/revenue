WITH blog_views AS ( 
SELECT
user_id,
session_id,
DATE_TRUNC('d',CONVERT_TIMEZONE('PST8PDT',time)) as pageview_date,
BOOL_OR(path ILIKE '%blog%') AS visited_blog, --visited the blog and the signup page in the same session
BOOL_OR(path ILIKE '%signup%') AS visited_signup_page
FROM main_production.pageviews 
WHERE CONVERT_TIMEZONE('PST8PDT',time) >= '2021-05-20' --experiment start date
GROUP BY 1,2,3),

agg AS (
SELECT 
	pageview_date,
	h.user_id,
	be.inferred_email,
	be.inferred_person_id,
	0 AS variation --binary label for control
FROM main_production.users h 
LEFT JOIN bench_enhanced.heap_user_identity be
	ON h.user_id = be.heap_user_id --grab some identifying fields
INNER JOIN blog_views pv --ensure the person from the experiment has been on the blog and naviated to the signup page
	ON h.user_id = pv.user_id
	AND visited_blog IS TRUE
	AND visited_signup_page IS TRUE
WHERE npknwmulqdqcx22kufsldg = 0 --grab all control users in the experiment
	AND NVL(be.inferred_email,'a') NOT ILIKE '%test%' 
	AND NVL(be.inferred_email,'a') NOT ILIKE '%bench.co%'
	AND NVL(be.inferred_email,'a') NOT ILIKE '%tamarablagojevic%'
	AND NVL(be.inferred_email,'a') NOT ILIKE '%ignore%'
	
UNION

SELECT 
	DATE_TRUNC('d',CONVERT_TIMEZONE('PST8PDT',time)) as pageview_date,
	pv.user_id,
	be.inferred_email,
	be.inferred_person_id,
	1 AS variation --binary label for variation
FROM main_production.airkit_page_view pv 
LEFT JOIN bench_enhanced.heap_user_identity be --grab some identifying fields
	ON pv.user_id = be.heap_user_id
WHERE page_name ILIKE '1 - Sign Up Page' --grab all users who went through the airkit sign up
	AND view_name = '1 - Sign Up Page'
	AND landing_page ILIKE '%blog%'
	AND CONVERT_TIMEZONE('PST8PDT',time) >= '2021-05-20' --experiment start date
	AND NVL(be.inferred_email,'a') NOT ILIKE '%test%' 
	AND NVL(be.inferred_email,'a') NOT ILIKE '%bench.co%'
	AND NVL(be.inferred_email,'a') NOT ILIKE '%tamarablagojevic%'
	AND NVL(be.inferred_email,'a') NOT ILIKE '%ignore%'
),

agg2 AS(
SELECT DISTINCT *,
	MIN (pageview_date) OVER (PARTITION BY user_id) as min_pageview, 
	MIN (variation) OVER (PARTITION BY user_id) as min_var,
	MAX (variation) OVER (PARTITION BY user_id) as max_var
FROM agg
)


SELECT 
	pageview_date,
	user_id,
	c.id as client_id,
	inferred_person_id,
	variation,
	ld.lead_created_datetime_pstpdt,
	sfo.torq_review_call_booked__c,
	CONVERT_TIMEZONE('PST8PDT',sfo.closedate) as closedate_pstpdt,
	sfo.stagename,
	sfo.iswon,
	CASE WHEN inferred_email IS NOT NULL AND ld.email IS NULL THEN 1 ELSE 0 END AS dropoff, --This assumes that the person entered their email through the SUF (classic or airkit), and did not enter their email subsequently somewhere else (like a resource)
	inferred_email,
	ld.email
FROM agg2 
LEFT JOIN revenue.lead_data ld
	ON agg2.inferred_email = ld.email
	AND ld.lead_created_datetime_pstpdt >= pageview_date
LEFT JOIN salesforce.sf_opportunity sfo
	ON ld.opp_id = sfo.id
	AND torq_review_call_booked__c >= pageview_date
	AND CONVERT_TIMEZONE('PST8PDT',sfo.closedate) >= pageview_date
LEFT JOIN mainapp_production_v2.client c
	ON agg2.inferred_person_id = c.principalpersonid
WHERE min_var = max_var --ensures everyone saw only one variation
	AND min_pageview = pageview_date 
;
