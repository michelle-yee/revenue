
SELECT DISTINCT
	conversion_date,
	DATE_TRUNC('month',conversion_date) themonth,
	bench_id,
	lead_id,
	opp_id,
	channel,
	conversion_credit,
	conversion_type,
	CASE 
		WHEN channel IN ('other_gp',
			'display',
			'competitor',
			'youtube',
			'non_branded_search',
			'branded_search',
			'podcast',
			'facebook',
			'reddit') THEN 'Paid'
		WHEN channel IN (
			'email',
			'search_content',
			'search_core',
			'social') THEN 'Inbound'
		ELSE channel
	END AS channel1
	--,sfu.name AS rep_name
FROM revenue.v_mcod_attributions_long_unallocated mcod
LEFT JOIN salesforce.sf_lead ld ON mcod.lead_id = ld.id
LEFT JOIN salesforce.sf_user sfu ON ld.ownerid = sfu.id
WHERE conversion_date >= '2021-01-01'
