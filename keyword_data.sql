WITH data AS(
SELECT
	DATE_TRUNC('month',conversion_date) as month,
	SUM(conversion_credit),
	conversion_type,
	utm_term
FROM mcod_v2.attributions
WHERE utm_term IS NOT NULL
	AND month >= ('2020-01-01')
	AND conversion_type <> 'REVIEW CALL'
GROUP BY 1,3,4), --data from all utm sources

pivot as (
SELECT
 	month,
 	CASE WHEN conversion_type = 'LEAD' then sum ELSE 0 END as leads,
 	CASE WHEN conversion_type = 'TRIAL' then sum ELSE 0 END as trials,
 	CASE WHEN conversion_type = 'CLIENT' then sum ELSE 0 END as clients,
 	utm_term
 FROM data
 ),

 uniques as
 (SELECT DISTINCT
 	month,
 	SUM(leads) OVER (PARTITION BY utm_term, month) as leads,
 	SUM(trials) OVER (PARTITION BY utm_term, month) as trials,
 	SUM(clients) OVER (PARTITION BY utm_term, month) as clients,
 	utm_term
 FROM pivot
 ORDER BY month, utm_term) --rolls up pivot data

 SELECT * FROM uniques where clients >=1 ORDER BY month, utm_term --pull keywords with at least 1 client
 ;
