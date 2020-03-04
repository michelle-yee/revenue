WITH scores AS
(
SELECT
	f.*,
	sfl.createddate,
	sfl.newvalue__string,
	sfl.oldvalue__string
FROM revenue.v_scored_lead_funnel f
LEFT JOIN salesforce.sf_leadhistory sfl
	ON ls_sal_lead_id = LEFT(sfl.leadid,15)
	AND sfl.field ILIKE '%score%'
)
,


aggregate as
(
SELECT scores.*,
	l.lead_score_rating__c,
	l.lead_scoring_lead__c,
	l.lead_assignment_date__c,
	CASE
		WHEN NVL(l.lead_assignment_date__c,l.first_lead_assignment_date__c) < NVL(l.mql_last_date_time__c , l.mql_first_date_time__c) THEN NVL(l.mql_last_date_time__c , l.mql_first_date_time__c)
		ELSE NVL(l.lead_assignment_date__c,l.first_lead_assignment_date__c)
	END as custom_date,
	DATE_DIFF ('m', custom_date, scores.createddate) as date_diff

FROM scores
LEFT JOIN salesforce.sf_lead l
	ON ls_sal_lead_id = LEFT (l.id,15)
),

summary as
(
SELECT
	*,
	CASE
		WHEN min_date_diff >0 THEN oldvalue__string
		WHEN min_date_diff <0 THEN newvalue__string
	END as assignment_lead_score
FROM
	(SELECT *,
		MIN(date_diff) OVER( PARTITION BY ls_sal_lead_id) as min_date_diff,
		(CASE WHEN ls_sal_lead_conversion IS NOT NULL THEN 1 ELSE 0 END) as leads,
		(CASE WHEN ls_sal_trial_conversion IS NOT NULL THEN 1 ELSE 0 END) as trials,
		(CASE WHEN ls_sal_client_conversion IS NOT NULL THEN 1 ELSE 0 END) as clients
	FROM aggregate)
WHERE min_date_diff = date_diff
)


SELECT
	assignment_lead_score,
	1.0*SUM(trials)/SUM(leads) as LT,
	SUM(leads) as leads,
	SUM(trials) as trials,
	SUM(clients) as clients
FROM summary
GROUP BY assignment_lead_score
ORDER BY assignment_lead_score;
