--use this to check leads against the lead definition
SELECT 
	leadsource,
	providedemail__c,
	providedcompanydetails__c,
	createdbenchaccount__C,
	isconverted,
	status,
	recordtypeid,
	phone,
	loss_reason__c,
	owner_role__c,
	potential_date__c,
	converteddate,
	isdeleted
FROM salesforce.sf_lead WHERE id ILIKE '%%';
