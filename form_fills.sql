SELECT 
        f.activitydate,
        l.sfdcleadid,
        email,
        f.query_parameters,
        f.referrer_url,
        f.form_fields
FROM marketo_v2.activities_fill_out_form f
LEFT JOIN marketo_v2.leads l
        ON f.leadid = l.id
WHERE f.primary_attribute_value_id = 2231 --AND activitydate >='2020-11-09'
;
