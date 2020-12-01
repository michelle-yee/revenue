"iwith uniques as(
SELECT
        unique_id,
        MIN(DATE_TRUNC('d',CONVERT_TIMEZONE('UTC','PST8PDT',touches.time))) OVER (PARTITION BY DATE_TRUNC('month',touches.time), unique_id) as touch_day, --counts the first time someone visits in a month as a unique
        DATE_TRUNC('month',touch_day) AS themonth, 
        DATE_DIFF('d',themonth,touch_day) AS diff,
        CASE WHEN diff <15 THEN '1' WHEN diff >14 THEN '2' END AS semi_month,
        lead_id, 
        lead_data.channel_allocation_stamp__c, 
        lead_data.actual_date_created__c, 
        lead_data.opp_id, 
        sfo.review_call_booked_boolean__c, 
        CASE WHEN sfo.review_call_booked_boolean__c IS TRUE THEN lead_id end as trial,
        sfo.stagename,
        CASE WHEN sfo.stagename = 'Client' then lead_id end as client
FROM mcod_v2.touches 
LEFT JOIN revenue.lead_data
        ON touches.lead_id = lead_data.id
LEFT JOIN salesforce.sf_opportunity sfo
        ON lead_data.opp_id = sfo.id
WHERE touches.utm_medium ILIKE '%gusto%' --utm parameters
AND touches.utm_source ILIKE '%partner%'
AND touches.utm_campaign ILIKE '%marketplace%'
AND touches.utm_content ILIKE '%lead-referral-page%'
)

SELECT 
        themonth,
        semi_month,
        COUNT(DISTINCT unique_id) as uniques,
        COUNT(distinct lead_id) as leads,
        COUNT(Distinct trial) as trials,
        COUNT (DISTINCT client) as clients
FROM uniques
GROUP BY 1,2
ORDER BY themonth, semi_month;
