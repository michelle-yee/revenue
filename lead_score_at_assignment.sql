
WITH scores AS
(
SELECT
	f.*,
	CONVERT_TIMEZONE('PST8PDT',sfl.createddate) as createddate,
	sfl.newvalue__string,
	sfl.oldvalue__string
FROM revenue.v_scored_lead_funnel f
LEFT JOIN salesforce.sf_leadhistory sfl
	ON ls_sal_lead_id = LEFT(sfl.leadid,15)
	AND sfl.field ILIKE '%score%'
WHERE f.ls_sal_lead_conversion >= '2019-04-01'

)
,
aggregate as
(
SELECT scores.*,
	l.lead_score_rating__c,
	l.lead_scoring_lead__c,
	CONVERT_TIMEZONE('PST8PDT',l.lead_assignment_date__c) as lead_assignment_date,
	CONVERT_TIMEZONE('PST8PDT',l.first_lead_assignment_date__c) as first_lead_assignment_date,
	CONVERT_TIMEZONE('PST8PDT',l.mql_first_date_time__c) as mql_first_date_time,
	CONVERT_TIMEZONE('PST8PDT',l.mql_last_date_time__c) as mql_last_date_time,
	l.test_1__c AS cta,
	CASE
		WHEN NVL(lead_assignment_date,first_lead_assignment_date) < NVL(mql_first_date_time,mql_last_date_time) THEN NVL(mql_first_date_time,mql_last_date_time)
		ELSE NVL(lead_assignment_date,first_lead_assignment_date)
	END as custom_date,
	DATE_DIFF ('s', custom_date, scores.createddate) as date_diff,
	ABS(date_diff) AS abs_diff
FROM scores
LEFT JOIN salesforce.sf_lead l
	ON ls_sal_lead_id = LEFT (l.id,15)
)
,
summary as
(
SELECT
	*,
	CASE
		WHEN date_diff > 0 THEN oldvalue__string
		WHEN date_diff < 0 THEN newvalue__string
	END as assignment_lead_score
FROM
	(SELECT *,
		MIN(abs_diff) OVER( PARTITION BY ls_sal_lead_id) as min_date_diff,
		(CASE WHEN ls_sal_lead_conversion IS NOT NULL THEN 1 ELSE 0 END) as leads,
		(CASE WHEN ls_sal_trial_conversion IS NOT NULL THEN 1 ELSE 0 END) as trials,
		(CASE WHEN ls_sal_client_conversion IS NOT NULL THEN 1 ELSE 0 END) as clients
	FROM aggregate)
WHERE min_date_diff = abs_diff
)

SELECT
	assignment_lead_score,
	1.0*SUM(trials)/SUM(leads) as LT,
	SUM(leads) as leads,
	SUM(trials) as trials,
	SUM(clients) as clients
FROM summary
WHERE (cta IS NULL OR cta NOT ILIKE '%BenchTax Info Call%')
GROUP BY assignment_lead_score
ORDER BY assignment_lead_score;
