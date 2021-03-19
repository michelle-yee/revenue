with all_data as(
SELECT DISTINCT 
	sf.id, 
	ld.email, 
	ld.lead_created_datetime_pstpdt, 
	sf.raw_date,
	sessions.user_id,
	CONVERT_TIMEZONE('UTC','PST8PDT',sessions.time) as session_time,
--	MAX(session_time) OVER (PARTITION BY ld.email) as last_session,
	referrer,
	landing_page,
	utm_medium,
	utm_content
FROM revenue.mcod_attributions_sf sf
LEFT JOIN revenue.lead_data ld ON sf.id = ld.id
LEFT JOIN main_production.users ON ld.email = users._email
LEFT JOIN main_production.sessions ON users.user_id = sessions.user_id 
	AND CONVERT_TIMEZONE('UTC','PST8PDT',sessions.time) <= ld.lead_created_datetime_pstpdt
WHERE referrals = 1 
	AND lead_ind = 1 
	AND DATE_TRUNC('MONTH', raw_date) >='2021-01-01'),
	
clean_data as (
SELECT 
	id, 
	raw_date,
	DATE_TRUNC('month',raw_date) as themonth,
	session_time,
	MAX (session_time) over (partition by id) as last_session,
	CASE 
		WHEN referrer is null then 'null'
		WHEN referrer ILIKE '%hello.meetbench%' THEN 'hello.meetbench'
		WHEN referrer ILIKE '%facebook%' THEN 'facebook'
		WHEN referrer ILIKE '%aeoworks%' THEN 'aeoworks' 
		WHEN referrer ILIKE '%bench.co%' THEN 'bench.co'
		WHEN referrer ILIKE '%bing%' THEN 'bing'
		WHEN referrer ILIKE '%youtube%' THEN 'youtube'
		WHEN referrer ILIKE '%ebay%' THEN 'ebay'
		WHEN referrer ILIKE '%google%' THEN 'google'
		WHEN referrer ILIKE '%reddit%' THEN 'reddit'
		WHEN referrer ILIKE '%slack%' THEN 'slack'
		else REGEXP_REPLACE(SPLIT_part(referrer,'?',1),'/$','') end as referrer,
	landing_page,
	utm_medium,
	utm_content
FROM all_data
) 

SELECT * FROM clean_data where session_time = last_session or last_session is null;
