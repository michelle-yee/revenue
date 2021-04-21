SELECT DISTINCT 
	contact.user_id, 
	mp._email, ld.id, 
	channel_1, 
	credit_1, 
	channel_2, 
	credit_2
FROM main_production.conversion_provided_contact_details contact
LEFT JOIN  main_production.users mp
	ON contact.user_id = mp.user_id
LEFT JOIN revenue.lead_data ld
	ON mp._email ILIKE ld.email
--LEFT JOIN revenue.attributions_by_channel ac
--	ON ld.id = ac.lead_id
WHERE landing_page ILIKE '%bench.co/go/professional-bookkeepers/%' 
	AND DATE_TRUNC('d',time) >='2021-04-1' 
	AND (utm_campaign ILIKE '%rd%' OR utm_campaign is null)
	
;
