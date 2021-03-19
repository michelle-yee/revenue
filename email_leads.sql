SELECT lead_id, lead_data.email, utm_medium, utm_source, utm_campaign, landing_page, referrer, conversion_type_id
from REVENUE.mcod_attributions_translated  mat
LEFT JOIN revenue.lead_data 
	on lead_id = id
where thedate = '2021-03-13 0:00:00'  --aka the week
  and mat.email > 0;
