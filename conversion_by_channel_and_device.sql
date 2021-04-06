WITH agg AS (
SELECT mcod.*, channel
FROM mcod_v2.attributions mcod
LEFT JOIN revenue.v_mcod_attributions_long_unallocated m --recovering channel mapping
	ON mcod.lead_id = m.lead_id
	AND mcod.conversion_type ILIKE m.conversion_type
	AND mcod.conversion_date = m.conversion_date
	AND mcod.conversion_credit = m.conversion_credit
WHERE date_trunc('month',mcod.conversion_date) = '2021-02-01') -- date scope

SELECT 
	conversion_type,
	channel,
	device_type,
	SUM(conversion_credit)
FROM agg
WHERE channel IN ('search_content','search_core') --channel specific
GROUP BY 1,2,3
ORDER BY 1,2,3;
