WITH camp AS(
SELECT 
        NVL(sfc.leadid, ld.id) as clean_leadid,
        CASE WHEN ld.lead_created_datetime_pstpdt >= CONVERT_TIMEZONE('PST8PDT',sfc.createddate) 
                THEN lead_created_datetime_pstpdt END AS lead_created_datetime_pstpdt,
        CONVERT_TIMEZONE('PST8PDT',sfc.createddate) as campaign_createddate,
        ld.actual_date_created__c,
        sfc.email as campaign_email,
        ld.cta,
        ld.leadsource
FROM salesforce.sf_campaignmember sfc
LEFT JOIN revenue.lead_data ld
        ON (sfc.leadid = ld.id OR sfc.email = ld.email)
--        AND lead_created_datetime_pstpdt >= CONVERT_TIMEZONE('PST8PDT',sfc.createddate)
WHERE campaignid = '7014R0000018jTFQAY'), --experiment campaign

forms AS(
SELECT 
        f.activitydate,
        l.sfdcleadid,
        l.email,
        f.query_parameters,
        f.referrer_url,
        f.form_fields
FROM marketo_v2.activities_fill_out_form f
LEFT JOIN marketo_v2.leads l
        ON f.leadid = l.id
WHERE f.primary_attribute_value_id = 2083 AND activitydate >= '2021-04-21') --experiment start

SELECT DISTINCT
        clean_leadid,
        lead_created_datetime_pstpdt,
        campaign_createddate,
        cta,
        abc.conversion_date,
        abc.conversion_type,
        abc.channel_1,
        abc.credit_1,
        abc.channel_2,
        abc.credit_2,
        forms.activitydate as consult_form
FROM camp
LEFT JOIN revenue.attributions_by_channel abc
        ON camp.clean_leadid = abc.lead_id
        AND conversion_date >= DATE_TRUNC('day',campaign_createddate)
        AND (camp.lead_created_datetime_pstpdt IS NOT NULL OR camp.lead_created_datetime_pstpdt IS NULL and channel_1 = 'scored') --excludes people who completed the quiz post lead conversion and pre trial/client conversion
LEFT JOIN forms
        ON camp.clean_leadid = forms.sfdcleadid
WHERE campaign_email NOT ILIKE '%test%'
        AND campaign_email NOT ILIKE '%bench.co%'
        AND campaign_email NOT IN ('excelapril21@gmail.com','elorn.tersla.moterz@gmail.com','mackenzie.ford@gmail.com','dennisapril20@gmail.com')
ORDER BY 2,1,5
;
