WITH uniques as (
SELECT
	DATE_TRUNC('month',touches.time) as thedate,
--	 DATE_ADD('d',-2,DATE_TRUNC('w',DATE_ADD('d',2,time))) as thedate,
	touches.time thetime,
	FIRST_VALUE(touches.time)
		OVER (PARTITION BY thedate,unique_id
			  ORDER BY touches.time
			  ROWS BETWEEN unbounded preceding AND unbounded following) as min_time,
	ROW_NUMBER()
		OVER (PARTITION BY thedate,unique_id,thetime
			  ORDER BY touches.time asc) as index,
	unique_id,
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
WHERE conversion_type_id=1
) as att
ON touches.unique_id = att.ident
WHERE (lead_conversion IS NULL OR thetime <= lead_conversion)
AND thedate >= '2017-12-30'
AND touches.landing_page NOT ILIKE '%/careers%'
AND touches.landing_page NOT ILIKE '%nginx%'
AND DATE_TRUNC('d',touches.time) <= LAST FRIDAY ---
)
, mapping as (
SELECT
	thedate,
	unique_id,
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
			(medium = 'ORGANIC' AND source IN ('GOOGLE','BING','UNIDENTIFIABLE SEARCH ENGINE','SYLLABUS')
			AND COALESCE(utm_medium,'NULL') NOT ILIKE 'social')
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
	THEN 1 ELSE 0 END as SEARCH_CORE,
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
WHERE uniques.thetime = uniques.min_time
AND index=1
)
, mod_mapping as
(
SELECT
	thedate,
	unique_id,
	medium,
	submedium,
	utm_medium,
	source,
	utm_campaign,
	landing_page,
	FACEBOOK,BRANDED_SEARCH,NON_BRANDED_SEARCH,DISPLAY,YOUTUBE,REDDIT,PODCAST,COMPETITOR,OTHER_GP,
	CASE WHEN SEARCH_CORE+SEARCH_CONTENT=1 THEN 1 ELSE 0 END AS SEARCH,
	SEARCH_CORE,
	SEARCH_CONTENT,
	CASE WHEN SEARCH_CORE+SEARCH_CONTENT=1
		 AND SOURCE = 'GOOGLE' THEN 1 ELSE 0 END AS GOOGLE,
	CASE WHEN SEARCH_CORE+SEARCH_CONTENT=1
		 AND SOURCE = 'BING' THEN 1 ELSE 0 END AS BING,
	CASE WHEN SEARCH_CORE+SEARCH_CONTENT=1
		 AND SOURCE NOT IN ('GOOGLE','BING') THEN 1 ELSE 0 END AS OTHER_SEARCH,
	SOCIAL,
	EMAIL,
	DIRECT,
	UNKNOWN,
	DIRECT_SOCIAL,
	FACEBOOK+BRANDED_SEARCH+NON_BRANDED_SEARCH+DISPLAY+YOUTUBE+REDDIT+PODCAST+COMPETITOR+OTHER_GP+SEARCH+SOCIAL+EMAIL+DIRECT+UNKNOWN+DIRECT_SOCIAL as TOTAL
FROM mapping
)
,mcodv2 as (
	SELECT
	  thedate,
	  SUM(FACEBOOK) as FACEBOOK,
	  SUM(BRANDED_SEARCH) as BRANDED_SEARCH,
	  SUM(NON_BRANDED_SEARCH) as NON_BRANDED_SEARCH,
	  SUM(DISPLAY) as DISPLAY,
	  SUM(YOUTUBE) as YOUTUBE,
	  SUM(REDDIT) as REDDIT,
	  SUM(PODCAST) as PODCAST,
	  SUM(COMPETITOR) as COMPETITOR,
	  SUM(OTHER_GP) as OTHER_GP,
	  SUM(SEARCH) as SEARCH,
	  SUM(SEARCH_CORE) as SEARCH_CORE,
	  SUM(SEARCH_CONTENT) as SEARCH_CONTENT,
	  SUM(GOOGLE) as GOOGLE,
	  SUM(BING) as BING,
	  SUM(OTHER_SEARCH) as OTHER_SEARCH,
	  SUM(SOCIAL) as SOCIAL,
	  SUM(EMAIL) as EMAIL,
	  SUM(DIRECT) as DIRECT,
	  SUM(UNKNOWN) as UNKNOWN,
	  SUM(DIRECT_SOCIAL) as DIRECT_SOCIAL
	FROM mod_mapping as mcod_pivot
	GROUP BY thedate
)
SELECT
	mcodv2.thedate as "Week_Starting",
	(FACEBOOK + BRANDED_SEARCH + NON_BRANDED_SEARCH + DISPLAY + YOUTUBE + REDDIT + PODCAST + COMPETITOR + OTHER_GP) as "G&P Total",
	FACEBOOK,BRANDED_SEARCH,NON_BRANDED_SEARCH,DISPLAY,YOUTUBE,REDDIT,PODCAST,COMPETITOR, OTHER_GP,
	(SEARCH + SOCIAL + EMAIL) as "O&C Total",
	SEARCH,SEARCH_CORE,SEARCH_CONTENT,GOOGLE,BING,OTHER_SEARCH,SOCIAL,EMAIL,
	DIRECT,
	UNKNOWN,
	DIRECT_SOCIAL,
	("G&P Total" + "O&C Total" + DIRECT + UNKNOWN + DIRECT_SOCIAL) as "Total"
FROM mcodv2
ORDER BY 1 ASC, 2 ASC
