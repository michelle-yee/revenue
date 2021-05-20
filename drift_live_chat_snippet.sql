--funnel counts for people who went through drift aka live chat 

SELECT 
	date_trunc('month',lead_created_datetime_pstpdt), 
	COUNT (distinct ld.id) as leads,
	COUNT(DISTINCT trials.id) as trials,
	COUNT(DISTINCT clients.id) as clients
FROM revenue.lead_data ld
LEFT JOIN salesforce.sf_opportunity trials ON ld.opp_id = trials.id
	AND date_trunc('month',lead_created_datetime_pstpdt) = date_trunc('month',trials.torq_review_call_booked__c)
LEFT JOIN salesforce.sf_opportunity clients ON ld.opp_id = clients.id
	AND date_trunc('month',lead_created_datetime_pstpdt) = date_trunc('month',clients.closedate)
	AND clients.stagename = 'Client'
	AND clients.iswon
WHERE ld.LEADSOURCE = 'Live Chat'
GROUP BY 1;
