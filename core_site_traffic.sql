WITH uniques as (
SELECT
	DATE_TRUNC('month',touches.time) as theweek,
--	DATE_ADD('d',-2,DATE_TRUNC('w',DATE_ADD('d',2,time))) as theweek,       --toggle between date scopes
	DATE_TRUNC('d',touches.time) as thedate,
	touches.time thetime,
	unique_id,
	total_touch_count,
	source,
	medium,
	submedium,
	utm_medium,
	utm_source,
	utm_campaign,
	landing_page,
	att.lead_date as lead_conversion
FROM mcod_v2.touches
LEFT JOIN (
SELECT unique_id as ident,conversion_date as lead_date
FROM mcod_v2.attributions
WHERE conversion_type_id=1 --
) as att
ON touches.unique_id = att.ident
WHERE (lead_conversion IS NULL OR thetime <= lead_conversion)
AND thedate >= '2018-01-01'
AND touches.landing_page NOT ILIKE '%/careers%'
AND touches.landing_page NOT ILIKE '%nginx%'
AND thedate <= '2020-06-19' ---
)
, mapping as (
SELECT
	thedate,
	theweek,
	thetime,
	unique_id,
	total_touch_count,
	medium,
	submedium,
	utm_medium,
	source,
	utm_campaign,
	landing_page,
	CASE WHEN
		(medium = 'GROWTH & PAID' AND submedium = 'OTHER' AND source = 'FACEBOOK' AND utm_campaign NOT ILIKE '%awareness%')
		THEN 1 ELSE 0 END as FACEBOOK,
	CASE WHEN
		(medium IN ('GROWTH & PAID','OTHER') AND submedium = 'BRANDED SEARCH' AND utm_medium NOT ILIKE '%display%')
			OR
   	(medium IN ('GROWTH & PAID','OTHER')
			AND submedium = 'OTHER'
			AND source IN ('GOOGLE','BING','OTHER')
			AND (utm_campaign ILIKE '%_brand%' OR utm_campaign = 'Bing - Search Brand USA (Engine)')
			AND utm_campaign NOT ILIKE '%_nonbrand%'
			AND utm_campaign <> 'Bing - Search Non-Brand USA (Engine)'
			AND utm_medium ='cpc')
		THEN 1 ELSE 0 END as BRANDED_SEARCH,
	CASE WHEN
		(medium IN ('GROWTH & PAID','OTHER')
			AND submedium = 'NON-BRANDED SEARCH'
			AND utm_medium = 'cpc'
			AND utm_campaign ILIKE '%_nonbrand%'
			AND utm_campaign NOT ILIKE '%competitor%'
			AND utm_campaign NOT ILIKE '%quickbooks%')
		OR
		(medium IN ('GROWTH & PAID','OTHER')
			AND submedium = 'OTHER'
			AND source IN ('GOOGLE','BING','OTHER')
			AND utm_medium = 'cpc'
			AND utm_campaign NOT ILIKE '%competitor%'
			AND utm_campaign NOT ILIKE '%quickbooks%'
			AND (utm_campaign ILIKE '%_nonbrand%' OR utm_campaign = 'Bing - Search Non-Brand USA (Engine)'))
		THEN 1 ELSE 0 END as NON_BRANDED_SEARCH,
	CASE WHEN
		(medium IN ('GROWTH & PAID','OTHER') AND submedium='DISPLAY') OR
		(source <> 'YOUTUBE' AND submedium='OTHER' AND utm_medium ILIKE '%display%') OR
		(medium = 'GROWTH & PAID' AND submedium = 'OTHER' AND source = 'FACEBOOK' AND utm_campaign ILIKE '%awareness%')
		THEN 1 ELSE 0 END as DISPLAY,
	CASE WHEN
		(medium = 'GROWTH & PAID' AND source='YOUTUBE' AND submedium <> 'COMPETITOR')
		THEN 1 ELSE 0 END as YOUTUBE,
	CASE WHEN
		(medium = 'GROWTH & PAID' AND source='REDDIT')
		THEN 1 ELSE 0 END as REDDIT,
	CASE WHEN
		(medium = 'GROWTH & PAID' AND submedium = 'PODCAST'
		 -- AND landing_page NOT ILIKE '%/login%'
		 AND (landing_page IS NULL OR landing_page NOT ILIKE '%/careers%'))
		THEN 1 ELSE 0 END as PODCAST,
	CASE WHEN
		medium IN ('GROWTH & PAID','OTHER')
		AND (utm_campaign ILIKE '%_competitor%' OR utm_campaign ILIKE '%_quickbooks%')
		THEN 1 ELSE 0 END as COMPETITOR,
	CASE WHEN
		(medium = 'GROWTH & PAID' AND source = 'CAPTERRA') OR
		(medium = 'GROWTH & PAID' AND submedium = 'OTHER' AND source = 'QUORA') OR
		(medium = 'GROWTH & PAID' AND submedium = 'OTHER' AND source IN ('GOOGLE','BING','OTHER')
			AND utm_medium ILIKE 'cpc'
			AND( utm_campaign IS NULL OR
					(utm_campaign NOT ILIKE '%_nonbrand%' AND
					utm_campaign NOT ILIKE '%_brand%' AND
					utm_campaign NOT ILIKE '%_competitor%' AND
					utm_campaign NOT ILIKE '%_quickbooks%')
				)
		) OR
		(medium = 'OTHER' AND submedium = 'OTHER' AND source IN ('OTHER','BING')
			AND utm_medium = 'cpc' AND utm_campaign NOT ILIKE '%brand%') OR
		(medium = 'OTHER' AND submedium = 'OTHER' AND source = 'LINKEDIN'
			AND utm_medium = 'cpc' AND utm_campaign = 'us_prospecting_linkedIn_rd') OR
		(medium = 'OTHER' AND submedium = 'OTHER' AND source = 'GOOGLE' AND utm_campaign ILIKE '%gmb%')
		THEN 1 ELSE 0 END as OTHER_GP,
	CASE WHEN
		(
			(medium = 'ORGANIC' AND /*source IN ('GOOGLE','BING','UNIDENTIFIABLE SEARCH ENGINE','SYLLABUS')*/
--			AND
			COALESCE(utm_medium,'NULL') NOT ILIKE 'social')
   		AND
			(
				(
				landing_page NOT LIKE '%bench.co/blog%' AND
				landing_page NOT LIKE '%bench.co/syllabus%'
				)
			AND NOT
			(thedate <= '2018-05-01' and
			landing_page IN
				(
				'bench.c/ecommerce/',
				'bench.co/fitness/',
				'bench.co/healthcare/',
				'bench.co/ecommerce/how-to-reduce-shipping-costs/',
				'bench.co/ecommerce/omnichannel-retailing-guide/',
				'bench.co/ecommerce/sales-tax-guide-online-sellers/',
				'bench.co/ecommerce/sales-tax-nexus-guide/',
				'bench.co/ecommerce/subscription-box-business/',
				'bench.co/ecommerce/tax-deductions-ecommerce/',
				'bench.co/ecommerce/sales-tax-basics/',
				'bench.co/fitness/how-to-find-more-clients-crossfit/',
				'bench.co/healthcare/tax-reductions-therapists/'
				)
			)
			)
		 -- AND landing_page NOT ILIKE '%/login%'
		 AND landing_page NOT ILIKE '%/careers%'
		)
	THEN 1 ELSE 0 END as CORE,
	CASE WHEN
		(
			(medium = 'ORGANIC' AND source IN ('GOOGLE','BING','UNIDENTIFIABLE SEARCH ENGINE','SYLLABUS')
			AND COALESCE(utm_medium,'NULL') NOT ILIKE 'social')
		OR
			(medium = 'OTHER' AND submedium = 'OTHER' AND source IN ('GOOGLE')
			AND utm_campaign ILIKE 'notb')
		)
		AND
		(
			(landing_page LIKE '%bench.co/blog%' OR landing_page LIKE '%bench.co/syllabus%')
			OR
			(thedate <= '2018-05-01' and
			landing_page IN
				(
				'bench.c/ecommerce/',
				'bench.co/fitness/',
				'bench.co/healthcare/',
				'bench.co/ecommerce/how-to-reduce-shipping-costs/',
				'bench.co/ecommerce/omnichannel-retailing-guide/',
				'bench.co/ecommerce/sales-tax-guide-online-sellers/',
				'bench.co/ecommerce/sales-tax-nexus-guide/',
				'bench.co/ecommerce/subscription-box-business/',
				'bench.co/ecommerce/tax-deductions-ecommerce/',
				'bench.co/ecommerce/sales-tax-basics/',
				'bench.co/fitness/how-to-find-more-clients-crossfit/',
				'bench.co/healthcare/tax-reductions-therapists/'
				)
			)
		)
	THEN 1 ELSE 0 END as SEARCH_CONTENT,
	CASE WHEN
		(medium = 'ORGANIC' AND utm_medium ILIKE 'social'
			AND (landing_page ILIKE '%blog%' OR landing_page ILIKE '%syllabus%')) OR
		(medium = 'OTHER' AND source IN ('GOOGLE','OTHER','YOUTUBE')
			AND utm_medium ILIKE '%social%'
			AND (landing_page ILIKE '%blog%' OR landing_page ILIKE '%syllabus%'))
		THEN 1 ELSE 0 END as SOCIAL,
	CASE WHEN
		(medium = 'E-MAIL') OR
		(medium = 'OTHER' AND
			(utm_medium ILIKE 'email' OR
			utm_medium = 'marketing_email'
			)
		)
	THEN 1 ELSE 0 END as EMAIL,
	CASE WHEN
		(medium = 'DIRECT' OR
		(medium = 'ORGANIC' AND submedium = 'OTHER' AND source = 'CAPTERRA') OR
		(medium = 'ORGANIC' AND submedium = 'OTHER' AND landing_page ILIKE '%/careers%') OR
		(
			medium = 'OTHER' AND submedium = 'OTHER' AND SOURCE IN ('OTHER','SHOPIFY')
			AND (utm_medium IS NULL OR utm_medium IN ('partner','referral','GuestPost','guestpost','Guide','article'))
			AND
			(
				utm_source IS NULL OR
				(
					utm_source NOT LIKE '%dribbble%' AND
					utm_source NOT LIKE '%producthunt%' AND
					utm_source NOT LIKE '%google%' AND
					utm_source NOT LIKE '%financesonline%' AND
					utm_source NOT LIKE '%hustle%')
			)
		)
		)
		THEN 1 ELSE 0 END as DIRECT,
		CASE WHEN medium = 'UNKNOWN' THEN 1 ELSE 0 END as UNKNOWN,
		CASE WHEN
		(medium = 'ORGANIC'
			AND utm_medium ILIKE '%social%'
			AND NOT (landing_page ILIKE '%blog%' OR landing_page ILIKE '%syllabus%')) OR
  	(medium = 'OTHER' AND source IN ('GOOGLE','OTHER','YOUTUBE')
			AND utm_medium ILIKE '%social%'
			AND NOT (landing_page ILIKE '%blog%' OR landing_page ILIKE '%syllabus%'))
    THEN 1 ELSE 0 END as DIRECT_SOCIAL
FROM uniques
)
, mod_mapping as
(
SELECT
	thedate,
	theweek,
	thetime,
	unique_id,
	total_touch_count,
	CASE WHEN CORE = 1 THEN DENSE_RANK () OVER (PARTITION BY unique_id,thedate,core ORDER BY thetime) END as core_view,
	medium,
	submedium,
	utm_medium,
	source,
	utm_campaign,
	landing_page,
	FACEBOOK,BRANDED_SEARCH,NON_BRANDED_SEARCH,DISPLAY,YOUTUBE,REDDIT,PODCAST,COMPETITOR,OTHER_GP,
	CASE WHEN core+SEARCH_CONTENT=1 THEN 1 ELSE 0 END AS SEARCH,
	core,
	SEARCH_CONTENT,
	CASE WHEN core+SEARCH_CONTENT=1
		 AND SOURCE = 'GOOGLE' THEN 1 ELSE 0 END AS GOOGLE,
	CASE WHEN core+SEARCH_CONTENT=1
		 AND SOURCE = 'BING' THEN 1 ELSE 0 END AS BING,
	CASE WHEN core+SEARCH_CONTENT=1
		 AND SOURCE NOT IN ('GOOGLE','BING') THEN 1 ELSE 0 END AS OTHER_SEARCH,
	SOCIAL,
	EMAIL,
	DIRECT,
	UNKNOWN,
	DIRECT_SOCIAL,
	FACEBOOK+BRANDED_SEARCH+NON_BRANDED_SEARCH+DISPLAY+YOUTUBE+REDDIT+PODCAST+COMPETITOR+OTHER_GP+SEARCH+SOCIAL+EMAIL+DIRECT+UNKNOWN+DIRECT_SOCIAL as TOTAL
FROM mapping
),

Core_traffic AS (
SELECT DISTINCT
	thedate,
	theweek,
	thetime,
	unique_id,
	total_touch_count,
	core_view,
	medium,
	submedium,
	utm_medium,
	source,
	utm_campaign,
	landing_page,
	core
	 FROM mod_mapping
WHERE core = 1 --visited the core site
	AND core_view = 1 --first core visit in the day
),

last_touch AS (
SELECT DISTINCT
	mm.thedate,
	mm.theweek,
	ct.unique_id,
	ct.total_touch_count-1 AS last_touch, --last touch before visiting core site
	mm.landing_page,
	mm.facebook,
	mm.branded_search,
	mm.non_branded_search,
	mm.display,
	mm.youtube,
	mm.reddit,
	mm.podcast,
	mm.competitor,
	mm.other_gp,
	mm.search,
	mm.core,
	mm.search_content,
	mm.google,
	mm.bing,
	mm.other_search,
	mm.social,
	mm.email,
	mm.direct,
	mm.unknown,
	mm.direct_social
FROM core_traffic ct
LEFT JOIN mod_mapping mm
	ON ct.unique_id = mm.unique_id
	AND ct.total_touch_count-1 = mm.total_touch_count
	AND ct.thedate = mm.thedate),

last_touch_sum as (    --rolling up by date scope
	SELECT
	 last_touch.theweek,
	  SUM(last_touch.FACEBOOK) as FACEBOOK,
	  SUM(last_touch.BRANDED_SEARCH) as BRANDED_SEARCH,
	  SUM(last_touch.NON_BRANDED_SEARCH) as NON_BRANDED_SEARCH,
	  SUM(last_touch.DISPLAY) as DISPLAY,
	  SUM(last_touch.YOUTUBE) as YOUTUBE,
	  SUM(last_touch.REDDIT) as REDDIT,
	  SUM(last_touch.PODCAST) as PODCAST,
	  SUM(last_touch.COMPETITOR) as COMPETITOR,
	  SUM(last_touch.OTHER_GP) as OTHER_GP,
	  SUM(last_touch.SEARCH_CONTENT) as SEARCH_CONTENT,
	  SUM(last_touch.SOCIAL) as SOCIAL,
	  SUM(last_touch.EMAIL) as EMAIL,
	  SUM(last_touch.DIRECT) as DIRECT,
	  SUM(last_touch.UNKNOWN) as UNKNOWN,
	  SUM(last_touch.DIRECT_SOCIAL) as DIRECT_SOCIAL
	FROM last_touch
	GROUP BY 1

),

core_sum as (.    --rolling up by date scope
	SELECT
		theweek,
		SUM(core) as core
	FROM core_traffic
	GROUP BY 1

)

SELECT * FROM core_sum
LEFT JOIN last_touch_sum ON core_sum.theweek = last_touch_sum.theweek;




SELECT DISTINCT
	ili.plan__id as plan,
	plan__interval AS interval,
--	CASE WHEN plan ILIKE '%\\_pro\\_%' THEN 'PRO'
--		 WHEN plan NOT ILIKE '%\\_pro\\_%' AND ili.metadata__benchtype = 'keepUp' THEN 'CORE'
--		 END AS type,
	CASE WHEN plan  ILIKE '%\\_tax\\_%' THEN 'BenchTax'
		 WHEN plan  ILIKE '%\\_pro\\_%' THEN 'Pro_Bookkeeping' ELSE 'Bookkeeping' END AS product,
--	ili.metadata__benchtype,
	gross_mrr,
	ili.currency,
	ili.amount*0.01

 FROM bench_stripe_integration_v2.invoice_line_items ili
 LEFT JOIN human_maintained.gross_mrrs mrr
	ON plan__id = mrr.stripe_subscription_plan_id
WHERE plan IS NOT NULL

	;
