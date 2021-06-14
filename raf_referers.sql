--this snippet tries to surface all the accounts with a successful referral

with refers as 
(SELECT DISTINCT
	MAX(ld.lead_created_datetime_pstpdt) OVER (PARTITION BY Referring_Contact_Account_ID__c) as last_referral,
	sfl.Referring_Contact_Account_ID__c 
FROM revenue.lead_data ld
INNER JOIN salesforce.sf_lead sfl ON ld.id = sfl.id
	AND sfl.recordtypeid = '01215000001YpzDAAS'
	AND sfl.Ambassador__Referred_By_Contact__c IS NOT NULL 
	AND sfl.leadsource = 'Referred By Other Unrelated Business')
	
	
SELECT DISTINCT
	sfa.id as account_id,
	sfa.benchid__c,
	CONVERT_TIMEZONE('PST8PDT',sfo.closedate) as clientdate,
	DATEDIFF('d',clientdate, current_date) as days_old,
	refers.last_referral
FROM salesforce.sf_account sfa
INNER JOIN refers ON refers.Referring_Contact_Account_ID__c = LEFT(sfa.id,15)
LEFT JOIN revenue.lead_data ld ON sfa.benchid__c = ld.bench_id__c
LEFT JOIN salesforce.sf_opportunity sfo ON ld.opp_id = sfo.id
AND stagename = 'Client'
