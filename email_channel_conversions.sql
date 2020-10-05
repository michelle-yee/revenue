SELECT 
	mcod.lead_client_id,
	mcod.lead_id, 
	mcod.opportunity_id, 
	mkto.email as mkto_email,
	sfl.email as sf_email, 
	mcod.email, 
	mcod.lead_client_id, 
	mkto.benchid__c
FROM revenue.mcod_attributions_translated mcod
LEFT JOIN salesforce.sf_lead sfl
	ON mcod.lead_id = sfl.id
LEFT JOIN marketo_v2.leads mkto
	ON mcod.lead_id = mkto.sfdcleadid
WHERE mcod.email > 0 
and date_trunc('month', conversion_date) = ('2020-09-01') --conversion date
and conversion_type_id = 2 --trials
