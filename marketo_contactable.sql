--This view indicates which emails would be considered as contactable and/or prospect based on Marketo suppression filters, and is used for email marketing benchmarks

WITH non_contactable_prospects AS(
SELECT
	id,
	email
FROM {{ source('marketo','leads') }}
WHERE
	(
	leadsource__c = 'Accountant'
	OR unsubscribed IS TRUE
	OR emailinvalid = 'TRUE'
	OR account_type__c IN ('Vendor', 'Business Partner', 'Cancelled - Follow Up Needed', 'Parent', 'Initial Referral', 'Qualified',
		'Agreement Sent / Received', 'Legal Review', 'Partner', 'Terminated',
		'Client Care - Active', 'Client', 'Client Care - Churning', 'Trial with CC', 'Reactivation - Trial with CC') --prospect specific supressions
	OR email ILIKE '%bench.co%'
	OR leadsource IN ('Partner List', 'List Upload','Test Lead')
	OR email_opt_out_formula__c = 'TRUE'
	OR email_opt_out__c IN ('True','true','yes')
	OR do_not_contact__c = 'TRUE'
	OR (reseller__c = 'TRUE' AND leadsource = 'Freshbooks')
	OR (reseller_lead__c = 'TRUE' AND leadsource = 'Freshbooks')
	OR emailinvalid = 'TRUE'
	OR bench_account_status__c IN ('Catch Up', 'Keep up', 'KEEP_UP', 'CATCH_UP') --prospect specific supressions
	OR PRO_Email_Opt_Out__c = 'TRUE'
	)
),


non_contactable AS (
SELECT
	id,
	email
FROM {{ source('marketo','leads') }}
WHERE
	(
	leadsource__c = 'Accountant'
	OR unsubscribed IS TRUE
	OR emailinvalid = 'TRUE'
	OR account_type__c IN ('Vendor', 'Business Partner', 'Cancelled - Follow Up Needed', 'Parent', 'Initial Referral', 'Qualified',
		'Agreement Sent / Received', 'Legal Review', 'Partner', 'Terminated' )
	OR email ILIKE '%bench.co%'
	OR leadsource IN ('Partner List', 'List Upload','Test Lead')
	OR email_opt_out_formula__c = 'TRUE'
	OR email_opt_out__c IN ('True','true','yes')
	OR do_not_contact__c = 'TRUE'
	OR (reseller__c = 'TRUE' AND leadsource = 'Freshbooks')
	OR (reseller_lead__c = 'TRUE' AND leadsource = 'Freshbooks')
	OR emailinvalid = 'TRUE'
	OR PRO_Email_Opt_Out__c = 'TRUE'
	)
),


non_contactable_type as(
SELECT
	id,
	email,
	CASE
		WHEN recordtypeid IS NULL AND recordtypeid_lead = '01215000001YpzDAAS' THEN 0
		WHEN recordtypeid IS NULL AND recordtypeid_lead =  '0121C000001cxHoQAI' THEN 0
		WHEN recordtypeid_lead NOT IN ('01215000001YpzDAAS','0121C000001cxHoQAI') AND recordtypeid NOT IN ('01215000001YpzDAAS','0121C000001cxHoQAI') THEN 1
		WHEN recordtypeid IS NULL AND recordtypeid_lead IS NULL THEN 1
		ELSE 0
	END as non_contactable
FROM {{ source('marketo','leads') }}
WHERE non_contactable = 1
),


non_contactable_country as (
SELECT
	id,
	email,
	NVL(REPLACE(country,'Unknown',''),'') ||
		NVL(REPLACE(country__c,'Unknown',''),'') ||
		NVL(REPLACE(country__c_account,'Unknown',''),'') ||
		NVL(REPLACE(inferredcountry,'Unknown',''),'') as aggregated_country
FROM {{ source('marketo','leads') }}
WHERE COALESCE(country,country__c,country__c_account,inferredcountry) IS NOT NULL
	AND aggregated_country<>''
	AND aggregated_country NOT LIKE '%US%'
	AND aggregated_country NOT LIKE '%USA%'
	AND aggregated_country NOT LIKE '%U.S.%'
	AND aggregated_country NOT LIKE '%U.S.A.%'
	AND aggregated_country NOT LIKE '%United States%'
	AND aggregated_country NOT LIKE '%United States of America%'
	AND aggregated_country NOT LIKE '%united_states%'
	AND aggregated_country NOT LIKE '%United State%'
)

SELECT DISTINCT
	id,
	CASE WHEN REGEXP_COUNT(benchid__c,'^[0-9]+$') > 0 AND LEN(benchid__c) < 10 THEN benchid__c END AS benchid__c,
	email,
	CASE
		WHEN email IN (SELECT email FROM non_contactable) THEN 0
		WHEN email IN (SELECT email FROM non_contactable_country) THEN 0
		WHEN email IN (SELECT email FROM non_contactable_type) THEN 0
	ELSE 1 END AS contactable_ind,
	CASE
		WHEN email IN (SELECT email FROM non_contactable_prospects) THEN 0
		WHEN email IN (SELECT email FROM non_contactable_country) THEN 0
		WHEN email IN (SELECT email FROM non_contactable_type) THEN 0
	ELSE 1 END AS prospect_ind
FROM {{ source('marketo','leads') }}
WHERE email IS NOT NULL
