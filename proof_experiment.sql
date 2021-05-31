WITH agg AS(

SELECT DISTINCT
	CONVERT_TIMEZONE('PST8PDT',time) as pageview_date,
	experimentvariant,
	h.user_id,
	be.inferred_email,
	ld.lead_created_datetime_pstpdt,
	opp.torq_review_call_booked__c,
	CONVERT_TIMEZONE('PST8PDT',opp.closedate) as closedate,
	opp.stagename,
	opp.iswon,
	CASE 
		WHEN experimentvariant = 'control' THEN 0 
		WHEN experimentvariant = 'test' THEN 1
	END as clean_variant,
	MIN (clean_variant) OVER (PARTITION BY user_id) AS min_variant,
	MAX (clean_variant) OVER (PARTITION BY user_id) AS max_variant,
	MIN(pageview_date) OVER (PARTITION BY user_id) as min_pageview
FROM main_production.proof_x_experience_viewed h
LEFT JOIN bench_enhanced.heap_user_identity be
	ON h.user_id = be.heap_user_id
LEFT JOIN revenue.lead_attributions ld
	ON be.inferred_email ILIKE ld.email
	AND lead_created_datetime_pstpdt >= CONVERT_TIMEZONE('PST8PDT',h.time)
LEFT JOIN salesforce.sf_opportunity opp
	ON ld.opportunity_id = opp.id
	AND opp.torq_review_call_booked__c >= CONVERT_TIMEZONE('PST8PDT',h.time) 
WHERE h.campaignid ILIKE '-MZmtsK5qtzqHjl2agLQ' --experiment ID
	AND CONVERT_TIMEZONE('PST8PDT',h.time) >= '2021-05-13' --on or after experiment start date
	AND NVL(inferred_email,'a') NOT ILIKE '%bench.co%' 
	AND NVL(inferred_email,'a') NOT ILIKE '%test%'
	
)

SELECT * FROM agg 
WHERE pageview_date = min_pageview
AND min_variant = max_variant;
