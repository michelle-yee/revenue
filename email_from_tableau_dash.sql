--use this snippet to finds emails from the marketo dash with
SELECT DISTINCT email
FROM revenue.v_marketo_dashboard_v2 m 
WHERE m.primary_attribute_value ILIKE '%Nurture\\_V2%' --specific email sends
AND opened_id IS NOT NULL
AND sal_lead_id IS NOT NULL
AND sal_lead_created IS NOT NULL
AND DATEDIFF('d', opened_date_pstpdt, sal_lead_created) <= 14 --assignment delay window
;
