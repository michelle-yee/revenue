
--This query is used for AB tests on the Bench website
--It surfaces full funnel metrics including: sign-up flow dropouts, uniques, leads, trials and clients and filters out viewers that have seen the experiment page on multiple devices

WITH leads AS
(
SELECT ld.*, sfl.email as email1, sfo.review_call_booked_boolean__c
FROM salesforce_reports.lead_data ld
LEFT JOIN salesforce.sf_lead sfl
ON ld.id = sfl.id
LEFT JOIN salesforce.sf_opportunity sfo
ON ld.opp_id = sfo.id
WHERE ld.actual_date_created__c >= '2021-11-15' --lead conversion on/after experiment start date
),
dropouts AS
(
SELECT
	sfl.actual_date_created__c,
	sfl.id,
	sfl.benchid__c,
	LOWER(sfl.email) AS email,
	sfo.review_call_booked_boolean__c,
	sfo.id as opp_id,
	CASE WHEN sfo.stagename = 'Client' THEN
		 COALESCE(sfo.bookkeeping_mrr__c,0)+COALESCE(sfo.tax_mrr__c,0)+
		 CASE WHEN sfo.bench_pro_opp_status__c = 'Client'
		 THEN sfo.bench_pro_mrr__c ELSE 0 END
		 END as won_mrr
FROM salesforce.sf_lead sfl
	LEFT JOIN salesforce.sf_opportunity sfo	ON sfl.convertedopportunityid = sfo.id
	LEFT JOIN leads ON sfl.id = leads.id
WHERE
	leads.id IS NULL
AND
	(
	COALESCE(sfl.leadsource,'NULL') NOT IN ('Outbound','Test Lead','Outbound Referral','Radius',
											'Equity Structure Change','Unassigned: Merged','Inbound Partnership Interest',
											'Partnership Prospecting','Content','Re-engaged Contact','List Upload','NULL')
	OR (
		sfl.leadsource IN ('Content','List Upload')
		AND
			 (
			 COALESCE(sfl.providedemail__c,sfl.providedcompanydetails__c,sfl.createdbenchaccount__c) IS NOT NULL
			 OR sfl.isconverted IS TRUE
			 )
		 )
	)
AND sfl.recordtypeid = '01215000001YpzDAAS'
AND COALESCE(sfl.loss_reason__c,'NULL') NOT IN ('Fake lead / account','Missed Call - No Contact')
AND COALESCE(sfl.owner_role__c,'NULL') NOT LIKE 'Sales Operations'
AND NOT sfl.isdeleted
AND sfl.actual_date_created__c >= '2021-11-15' --SUF dropout on/after experiment start date
)
,
sample AS
(
SELECT
	u.user_id,
	u.id,
	u._email AS emails,
	u.ejzGen3XRsC4wyKlCWVaHg AS variation_id, --experiment ID
	MIN(u.ejzGen3XRsC4wyKlCWVaHg) OVER (PARTITION BY _email) as variation_min, --experiment ID
	MAX(u.ejzGen3XRsC4wyKlCWVaHg) OVER (PARTITION BY _email) as variations_max --experiment ID
FROM main_production.users u
WHERE u.ejzGen3XRsC4wyKlCWVaHg IS NOT NULL --experiment ID
) 
,
email_exclusions AS --emails who have seen multiple variations
(
SELECT * FROM sample WHERE variation_min <> variations_max AND emails IS NOT NULL
)
,
people AS
(
SELECT
	sample.*,
	map.id AS person_id,
	LOWER(map.email) AS person_email,
	mac.id AS client_id
FROM sample
LEFT JOIN mainapp_production_v2.person map
	ON sample.id = map.id
LEFT JOIN mainapp_production_v2.client mac
	ON map.id = mac.principalpersonid
)
,
views AS
(
SELECT
	pv.user_id,
	DATE_TRUNC('d',CONVERT_TIMEZONE('PST8PDT',pv.time)) AS view_day,
	pv.device_type
FROM main_production.pageviews pv
  INNER JOIN sample ON pv.user_id = sample.user_id
WHERE pv.time >= CONVERT_TIMEZONE('PST8PDT','UTC','2021-11-15 00:00:00') --site view on/after experiment start date
  AND pv.path LIKE '/' -- '/' is the homepage or add any page a paid ad leads to
)
,
retro_clients AS 
(
SELECT DISTINCT
	leads.opp_id,
	leads.Bench_ID__c,
	leads.email,
	opp.stagename,
	opp.iswon,
	opp.closedate
FROM leads 
LEFT JOIN salesforce.sf_opportunity opp
ON leads.opp_id = opp.id
WHERE stagename = 'Onboarded with Retro' AND opp.recordtypeid = '01215000000XB5dAAG' AND irs_cu_only_team__c
) 
,
aggregation AS
(
SELECT DISTINCT
	views.user_id,
--	people.person_email,
	variation_id,
	LISTAGG(DISTINCT views.device_type,', ') WITHIN GROUP (ORDER BY view_day) OVER (PARTITION BY views.user_id) as device,
	COUNT(DISTINCT views.device_type) WITHIN GROUP (ORDER BY view_day) OVER (PARTITION BY views.user_id) as device_count,
	FIRST_VALUE(leads.id) OVER (PARTITION BY views.user_id) as first_lead_id,
	FIRST_VALUE(dropouts.id) OVER (PARTITION BY views.user_id) as first_dropout_id,
	MAX(leads.won_mrr) OVER (PARTITION BY views.user_id) as max_mrr,
	MAX(CASE WHEN leads.review_call_booked_boolean__c THEN 1 ELSE 0 END) OVER ( PARTITION BY views.user_id) as trial_indicator,
	CASE WHEN r.opp_id IS NOT NULL THEN 1 ELSE 0 END as retro_client
FROM views
LEFT JOIN people
	ON views.user_id = people.user_id
LEFT JOIN leads
	ON (client_id = leads.bench_id__c OR person_email = leads.email1 OR people.emails = leads.email1)
	AND view_day <= leads.actual_date_created__c
LEFT JOIN dropouts
	ON (client_id = dropouts.benchid__c OR person_email = dropouts.email OR people.emails = dropouts.email)
	AND view_day <= dropouts.actual_date_created__c
LEFT JOIN retro_clients r
	ON (client_id = r.bench_id__c OR person_email = r.email OR people.emails = r.email)
WHERE 
	person_email IS NULL  
	OR people.emails IS NULL 
	OR (person_email NOT LIKE '%bench.co%' 
	AND person_email NOT LIKE '%test%' 
	AND people.emails NOT LIKE '%bench.co%' 
	AND people.emails NOT LIKE '%test%')
	AND person_email NOT IN (SELECT email_exclusions.emails FROM email_exclusions) --take out people who have seen multiple variations
	AND people.emails NOT IN (SELECT email_exclusions.emails FROM email_exclusions) --take out people who have seen multiple variations
)

SELECT
	variation_id,
	device,
	COUNT(DISTINCT first_dropout_id) as dropouts, --people who did not finish the SUF
	COUNT(DISTINCT user_id) as viewers,
	COUNT(DISTINCT first_lead_id) as leads,
	SUM(trial_indicator) as trials,
	SUM(CASE WHEN max_mrr IS NOT NULL THEN 1 ELSE 0 END) as clients
--	SUM(retro_client) as retro_clients
FROM aggregation
WHERE device_count = 1 --ensures the viewer has seen only 1 variation
GROUP BY 1,2
ORDER BY 1,2;
