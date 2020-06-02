with rawjoin AS (
SELECT DISTINCT
	email_sent,
	m.primary_attribute_value,
	sent_id,
	opened_id,
	clicked_id,
	email,
	ROW_NUMBER() OVER(PARTITION BY sent_id ORDER BY email_sent DESC) as number,
	CASE WHEN m.link ILIKE '%unsub%' THEN sent_id END AS unsub_id,
	CASE WHEN b.leadid IS NOT NULL THEN b.leadid end as bounce_h,
	CASE WHEN s.leadid IS NOT NULL THEN s.leadid end as bounce_s,
	CASE
		WHEN m.primary_attribute_value ILIKE '%syllabus%' THEN 1
		WHEN m.primary_attribute_value ILIKE '%\\_OPR\\_%' THEN 1
		ELSE 0 END AS trigger_ind
FROM revenue.v_marketo_dashboard_v2 m
LEFT JOIN marketo_v2.activities_email_bounced b
	ON sent_id = b.leadid
	AND DATE_TRUNC('d',email_sent) = DATE_TRUNC('d',CONVERT_TIMEZONE('PST8PDT',b.activitydate))
	AND m.primary_attribute_value = b.primary_attribute_value
LEFT JOIN marketo_v2.activities_email_bounced_soft s
	ON sent_id = s.leadid
	AND DATE_TRUNC('d',email_sent) = DATE_TRUNC('d',CONVERT_TIMEZONE('PST8PDT',s.activitydate))
	AND m.primary_attribute_value = s.primary_attribute_value
--WHERE DATE_TRUNC('month',email_sent) >= '2020-03-01' --specific month for general email metrics
WHERE DATE_TRUNC('month',email_sent) >= '2020-03-01' AND DATE_TRUNC('month',email_sent) <= '2020-04-01' --date range for engaged/unengaged
ORDER BY sent_id, email_sent
),
email_agg AS (
SELECT primary_attribute_value,
	COUNT(DISTINCT sent_id) as sent,
	COUNT(DISTINCT opened_id) as opened,
	COUNT(DISTINCT clicked_id) as clicked,
	COUNT(DISTINCT unsub_id) as unsubs,
	COUNT(DISTINCT bounce_h) as hard_bounces,
	COUNT(DISTINCT bounce_s) as soft_bounces
FROM rawjoin
--WHERE primary_attribute_value ILIKE '%CLNT%' --client emails only
--WHERE trigger_ind = 0 without trigger sends
--WHERE primary_attribute_value ILIKE '%webinar%' --webinar emails only
GROUP BY 1
),

frequency as (
SELECT DISTINCT COUNT(primary_attribute_value) OVER (PARTITION BY email) as total_sent, email FROM(
SELECT DISTINCT primary_attribute_value, email FROM rawjoin WHERE primary_attribute_value NOT ILIKE '%syllabus%' ))



--engaged
SELECT COUNT(DISTINCT opened_id) FROM (
SELECT *, count(unsub_id) OVER (PARTITION BY sent_id) as unsub_ind
FROM rawjoin
WHERE trigger_ind = 0)
WHERE unsub_ind = 0;


--unengaged
SELECT COUNT(DISTINCT sent_id) FROM(
	SELECT
	*,
	count(opened_id) OVER (PARTITION BY sent_id) as open_count,
	count(clicked_id) OVER (PARTITION BY sent_id) as click_count,
	MAX(number) OVER (PARTITION BY sent_id) as max
	FROM rawjoin
	WHERE trigger_ind = 0
)
WHERE
	open_count = 0 --restricted to no opens
	AND click_count = 0 --restricted to not clicked
	AND  number <= 3 --restricts to last 3 emails sent
	AND max >=3 --receieved at least 3 emails
	AND unsub_id IS NULL -- no unsubscribes
;



--agg frequency
SELECT COUNT(email),total_sent FROM frequency
GROUP BY total_sent;


--0 email sends

SELECT COUNT (DISTINCT email) FROM
(SELECT DISTINCT email FROM marketo_v2.leads WHERE (unsubscribed IS NULL OR unsubscribed = 'FALSE') AND (emailinvalid = 'FALSE' or emailinvalid IS NULL)
EXCEPT
SELECT DISTINCT email FROM revenue.v_marketo_dashboard_v2 WHERE DATE_TRUNC('month',email_sent) = '2020-04-01');


--general metrics
SELECT
	SUM(sent) as total_sent,
	SUM(opened) as total_opened,
	SUM(clicked) as total_clicked,
	SUM(unsubs) as total_unsubs,
	SUM(hard_bounces) as total_hard_bounces,
	SUM(soft_bounces) as total_soft_bounces
FROM email_agg
