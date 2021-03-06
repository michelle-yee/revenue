WITH leads AS
(
SELECT ld.*, sfl.email, sfo.review_call_booked_boolean__c
FROM revenue.lead_data ld
LEFT JOIN salesforce.sf_lead sfl
ON ld.id = sfl.id
LEFT JOIN salesforce.sf_opportunity sfo
ON ld.opp_id = sfo.id
WHERE ld.actual_date_created__c >= '2020-02-26' --
),
dropouts AS
(
SELECT
	sfl.actual_date_created__c,
	sfl.id,
	sfl.benchid__c,
	sfl.email,
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
ORDER BY actual_date_created__c DESC
),
sample AS
(
SELECT
	u.user_id,
	u.id,
	u._email AS email,
	u.BpEcsLLQTIuAqUemWarZbw AS variation_id --
FROM main_production.users u
WHERE u.BpEcsLLQTIuAqUemWarZbw IS NOT NULL --
),
people AS
(
SELECT
	sample.*,
	map.id AS person_id,
	map.email AS person_email,
	mac.id AS client_id
FROM sample
LEFT JOIN mainapp_production_v2.person map
	ON sample.id = map.id
LEFT JOIN mainapp_production_v2.client mac
	ON map.id = mac.principalpersonid
),
views AS
(
SELECT
	pv.user_id,
	DATE_TRUNC('d',CONVERT_TIMEZONE('PST8PDT',pv.time)) AS view_day,
	pv.device_type
FROM main_production.pageviews pv
WHERE view_day >= '2020-02-26' --
AND pv.path LIKE '/'
AND pv.user_id IN (SELECT user_id FROM sample)
),
aggregation AS
(
SELECT DISTINCT
	views.user_id,
	people.person_email,
	variation_id,
	LISTAGG(DISTINCT views.device_type,', ') WITHIN GROUP (ORDER BY view_day) OVER (PARTITION BY views.user_id) as device,
	COUNT(DISTINCT views.device_type) WITHIN GROUP (ORDER BY view_day) OVER (PARTITION BY views.user_id) as device_count,
	FIRST_VALUE(leads.id) OVER (PARTITION BY views.user_id) as first_lead_id,
	FIRST_VALUE(dropouts.id) OVER (PARTITION BY views.user_id) as first_dropout_id,
	MAX(leads.won_mrr) OVER (PARTITION BY views.user_id) as max_mrr,
	MAX(CASE WHEN leads.review_call_booked_boolean__c THEN 1 ELSE 0 END) OVER ( PARTITION BY views.user_id) as trial_indicator,
	(CASE
		WHEN mkt.path ILIKE '%request-a-bookkeeping-demo%' AND variation_id = 2 THEN 1
		ELSE 0
	END) AS demo_indicator
FROM views
LEFT JOIN people
	ON views.user_id = people.user_id
LEFT JOIN leads
	ON (client_id = leads.bench_id__c OR person_email ILIKE leads.email OR people.email ILIKE leads.email)
	AND view_day <= leads.actual_date_created__c
LEFT JOIN main_production.marketo_form_1500_submitted mkt
	ON views.user_id = mkt.user_id
	AND view_day <= DATE_TRUNC('d',CONVERT_TIMEZONE('PST8PDT',mkt.time))
LEFT JOIN dropouts
	ON (client_id = dropouts.benchid__c OR person_email ILIKE dropouts.email OR people.email ILIKE dropouts.email)
	AND view_day <= dropouts.actual_date_created__c
WHERE (person_email IS NULL
	OR people.email IS NULL
	OR person_email NOT LIKE '%bench.co%' AND person_email NOT LIKE '%test%' AND people.email NOT LIKE '%bench.co%' AND people.email NOT LIKE '%test%')
)
SELECT
	variation_id,
	device,
	COUNT(DISTINCT first_dropout_id) as dropouts, --people who did not finish the SUF
	COUNT(DISTINCT user_id) as viewers,
	COUNT(DISTINCT first_lead_id) as leads,
	SUM(trial_indicator) as trials,
	SUM(CASE WHEN max_mrr IS NOT NULL THEN 1 ELSE 0 END) as clients,
	SUM(demo_indicator) as demos
FROM aggregation
WHERE device_count = 1
--AND variation_id = 2 --exclude pages with previous button
--AND demo_indicator = 1 --demo button
GROUP BY 1,2
ORDER BY 1,2;
