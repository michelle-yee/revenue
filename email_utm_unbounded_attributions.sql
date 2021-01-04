WITH uniques AS (
SELECT DISTINCT email
FROM revenue.v_marketo_dashboard_v2 
WHERE primary_attribute_value 
ILIKE '%20-10-29\\_SAL\\_PROS\\_PRO\\_Nurture\\_Sales Lead Nurture - Leads%'
),
data AS
(
SELECT DISTINCT 
	/*pv.time, 
	pv.landing_page, 
	pv.utm_campaign,
	pv.utm_content,
	pv.utm_medium,
	pv.utm_source,
	pv.user_id, */
	sfl.id as lead_id,
	sfl.email,
	sfl.lead_created_datetime_pstpdt,
	sfo.review_call_booked_boolean__c,
	sfo.review_call_date_time__c,
	sfo.stagename
FROM main_production.pageviews pv
	LEFT JOIN main_production.users u
ON pv.user_id = u.user_id
	LEFT JOIN revenue.lead_data sfl
ON u._email ILIKE sfl.email
	LEFT JOIN salesforce.sf_opportunity sfo
ON sfl.opp_id = sfo.id
WHERE 
	utm_medium = 'marketing_email'
	AND utm_source = 'nurture_email'
	AND utm_campaign = 'salesnurture'
	AND utm_content = 'nurturelead'
)


SELECT 
	COUNT(DISTINCT lead_id) as leads,
	COUNT(DISTINCT CASE WHEN review_call_booked_boolean__c IS TRUE THEN lead_id END) as trials,
	COUNT (DISTINCT CASE WHEN stagename = 'Client' OR stagename = 'Onboarded with Retro' THEN lead_id END) AS clients
FROM data
ORDER BY leads DESC;
