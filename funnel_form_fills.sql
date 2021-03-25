with form_fill as (
SELECT DISTINCT
--	f.activitydate,
	l.sfdcleadid/*,
	f.query_parameters,
	f.referrer_url,
	f.form_fields*/
FROM marketo_v2.activities_fill_out_form f
LEFT JOIN marketo_v2.leads l
	ON f.leadid = l.id
WHERE f.primary_attribute_value_id = 2303),

SELECT 
	sfdcleadid,
	sfl.cta__c,
	sfo.torq_Review_Call_Booked__c,
	sfo.closedate,
	sfo.stagename,
	sfo.iswon
FROM form_fill
LEFT JOIN salesforce.sf_lead sfl ON form_fill.sfdcleadid = sfl.id 
LEFT JOIN salesforce.sf_opportunity sfo ON sfl.convertedopportunityid = sfo.id
WHERE sfdcleadid is not null
