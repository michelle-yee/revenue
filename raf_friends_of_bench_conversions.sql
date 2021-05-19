with leads as(
SELECT 
	pv.user_id, 
	be.inferred_email,
	pv.session_id, 
	ld.id,
	CONVERT_TIMEZONE('PST8PDT',pv.time) AS view_day,
	MIN(view_day) OVER (PARTITION BY inferred_email) AS min_view_day,
	CASE WHEN min_view_day <= ld.lead_created_datetime_pstpdt THEN ld.lead_created_datetime_pstpdt END as lead,
	landing_page
FROM main_production.pageviews pv 
LEFT JOIN bench_enhanced.heap_user_identity be
	ON pv.user_id = be.heap_user_id
LEFT JOIN revenue.lead_data ld
	ON be.inferred_email = ld.email
WHERE landing_page ILIKE '%bench.co/friends-of-bench/%' 
AND CONVERT_TIMEZONE('PST8PDT',pv.time) >= '2020-01-01'
AND (inferred_email NOT ILIKE '%test%' AND inferred_email NOT ILIKE '%bench.co%')
)
SELECT DISTINCT
	user_id,
	leads.id,
	session_id,
	view_day,
	DATE_TRUNC('month',view_day) AS view_month,
	lead,
	DATE_TRUNC('month',lead) AS lead_month,
	sfo.torq_review_call_booked__c as trial,
	DATE_TRUNC('month',torq_review_call_booked__c) as trial_month,
	CASE WHEN sfo.stagename = 'Client' AND sfo.isclosed AND sfo.iswon THEN closedate END as client,
	DATE_TRUNC('month',client) AS client_month,
	DATE_DIFF('d',view_day, lead),
	channel_1,
	CASE WHEN channel_2 IS NULL THEN 'null' ELSE channel_2 END AS channel_2,
	credit_1,
	credit_2
FROM leads
LEFT JOIN salesforce.sf_lead sfl
	ON sfl.id = leads.id
	AND lead IS NOT NULL --ensures the person viewed the page prior to lead creation
LEFT JOIN salesforce.sf_opportunity	sfo
	ON sfo.id = sfl.convertedopportunityid
	AND lead IS NOT NULL --ensures the person viewed the page prior to lead creation
LEFT JOIN revenue.attributions_by_channel R
	ON leads.id = R.lead_id
	and lead is not null
	and conversion_type = 'lead'
WHERE min_view_day = view_day 
AND (min_view_day <= lead OR lead is null)
AND (min_view_day <= trial OR trial is null)
AND (min_view_day <= client OR client is null);
