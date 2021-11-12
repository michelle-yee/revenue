--email metrics summary
--use this snippet to aggregate general Marketo metrics when assessing email campaigns

SELECT
	ed.primary_attribute_value,
	COUNT(DISTINCT ed.leadid) AS delivered_emails,
	COUNT(DISTINCT oe.leadid) AS opened_emails,
	COUNT(DISTINCT ce.leadid) AS recipients_who_clicked
FROM marketo_v2.activities_email_delivered ed
LEFT JOIN marketo_v2.activities_open_email oe
	ON ed.primary_attribute_value = oe.primary_attribute_value
LEFT JOIN marketo_v2.activities_click_email ce
	ON ed.primary_attribute_value = ce.primary_attribute_value
WHERE ed.primary_attribute_value ILIKE '%August Sales Leads Cohort Nurture%' --Marketo program name
GROUP BY 1;
