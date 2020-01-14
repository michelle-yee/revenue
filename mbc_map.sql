CREATE OR REPLACE VIEW revenue.mbc_map AS
(

------------------------------------ Mapping accounts together through the parent ID
WITH relationship AS
(SELECT
	sfa.id,
	sfa.parentid,
	sfa.benchid__c,
	sfa.name,
	sfa.recordtypeid,
	sfa.primary_contact_email__c,
	COUNT(sfa.id) OVER(PARTITION BY sfa.parentid) AS total_child
FROM salesforce.sf_account sfa
LEFT JOIN salesforce.sf_account sfap
	ON sfa.parentid = sfap.id
WHERE sfap.recordtypeid = '01215000001UUyRAAW' AND sfa.recordtypeid = '01215000001UUyRAAW'
AND sfa.parentid IS NOT NULL
AND sfa.isdeleted IS FALSE
),
------------------------------------ Appending the channel allocation of opps associated to the various accounts in 'relationship'
channel AS
(SELECT
	rel.id,
	rel.recordtypeid,
	rel.parentid,
	revlead.channel_allocation_stamp__c,
	revlead.leadsource,
	revlead.closedate
FROM relationship rel
LEFT JOIN revenue.lead_data revlead
	ON rel.id = revlead.accountid
),
------------------------------------- Indexing the order of opps to return an indication on whether or not channel allocations have correct structure
mbc AS
(SELECT
	rel.id,
	rel.parentid,
	rel.benchid__c,
	rel.name,
	rel.total_child,
	ch.channel_allocation_stamp__c,
	ch.leadsource,
	ch.closedate,
	RANK() OVER (PARTITION BY rel.parentid ORDER BY benchid__c ASC) AS client_order,
	CASE
		WHEN (ch.channel_allocation_stamp__c <> 'MBC' AND client_order = 1) THEN 0
		WHEN (client_order > 1 AND ch.channel_allocation_stamp__c = 'MBC') THEN 0
		WHEN (ch.channel_allocation_stamp__c IS NULL) THEN 0
		ELSE 1
		END as channel_test
FROM relationship rel
LEFT JOIN channel ch
	ON rel.id = ch.id
),
------------------------------------ Initial subscription opportunities
------------------------------------ Only for those accounts in 'mbc' where we didn't find an opp in revenue.lead_data
missing_opps AS
(SELECT DISTINCT
	sfo.id,
	sfo.accountid,
	sfo.leadsource,
	sfl.channel_allocation_stamp__c
FROM salesforce.sf_opportunity sfo
LEFT JOIN salesforce.sf_lead sfl
 	ON sfl.convertedopportunityid = sfo.id
INNER JOIN
	(SELECT DISTINCT id FROM mbc WHERE closedate IS NULL) as missing_opps
	ON sfo.accountid = missing_opps.id
WHERE sfo.recordtypeid IN ('0121C0000014DLQQA2','0121C0000014DLVQA2','01215000000XB5dAAG','0121C0000014DKhQAM')
	AND (sfo.iswon OR sfo.stagename = 'Client')
	AND sfo.leadsource NOT IN ('Equity Structure Change','Test Lead')
)

SELECT
	mbc.id,
	mbc.parentid,
	mbc.benchid__c,
	mbc.name,
	mbc.total_child,
	NVL (mbc.channel_allocation_stamp__c,missing_opps.channel_allocation_stamp__c) AS channel,
	NVL (mbc.leadsource,missing_opps.leadsource) AS leadsource,
	mbc.closedate,
	CASE WHEN (SUM(mbc.channel_test) OVER (PARTITION BY mbc.parentid))>0 THEN 'error' END AS test
FROM mbc
LEFT JOIN missing_opps
	ON mbc.id = missing_opps.accountid

ORDER BY parentid,closedate

)
WITH NO SCHEMA BINDING; 
