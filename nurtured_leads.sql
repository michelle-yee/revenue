-- This view is used to identify leads that once had a nurture status and whether they have since re-engaged

    WITH agg as(
    SELECT
        ld.id AS lead_id,
        ld.lead_created_datetime_pstpdt AS lead_date,
        sfo.torq_review_call_booked__c as trial_date,
        CONVERT_TIMEZONE('UTC','PST8PDT',sfo.closedate) as client_date,
        sfl.nurture_date__c as nurture_date,
       	sfl.re_engagement_datetime__c as reengagement_date,
        sfl.convertedopportunityid,
        sfo.id as opp_id,
        sfo.stagename,
        sfo.iswon,
        sfo.isclosed,
        CASE WHEN sfl.re_engagement_datetime__c > sfl.nurture_date__c then 1 ELSE 0 END AS reengaged
    FROM {{ ref('lead_data') }} ld
    LEFT JOIN {{ source('salesforce','sf_lead') }} sfl --add SF lead data b/c we need the nurture field which doesn't exist in revenue.lead_data
            ON ld.id = sfl.id
    LEFT JOIN {{ source('salesforce','sf_opportunity') }} sfo
            ON sfl.convertedopportunityid = sfo.id
    WHERE sfl.nurture_Date__c IS NOT NULL --we need all leads that have been nurtured at any point in time in their sales journey
            AND (sfl.nurture_date__c < sfo.torq_review_call_booked__c OR sfo.torq_review_call_booked__c IS NULL)--nuture must have happened before converting to trial/client
            AND (sfl.nurture_date__c < CONVERT_TIMEZONE('UTC','PST8PDT',sfo.closedate) OR sfo.closedate IS NULL)
     )
    SELECT
    	lead_id,
    	opp_id,
    	nurture_date AS conversion_date,
    	'nurture' AS conversion_type,
    	reengaged
    FROM agg
    UNION
    SELECT
    	lead_id,
    	opp_id,
    	reengagement_date AS conversion_date,
    	'reengagement' AS conversion_type,
    	reengaged
    FROM agg
    WHERE reengagement_date IS NOT NULL
    UNION
    SELECT
    	lead_id,
    	opp_id,
    	lead_date AS conversion_date,
    	'lead' AS conversion_type,
    	reengaged
    FROM agg
    WHERE lead_date IS NOT NULL
    UNION
    SELECT
    	lead_id,
    	opp_id,
    	trial_date AS conversion_date,
    	'trial' AS conversion_type,
    	reengaged
    FROM agg
    where trial_date IS NOT NULL
    UNION
    SELECT
    	lead_id,
    	opp_id,
    	client_date AS conversion_date,
    	'client' AS conversion_type,
    	reengaged
    FROM agg
    WHERE client_date IS NOT NULL
    AND stagename = 'Client'
    AND iswon
    AND isclosed
