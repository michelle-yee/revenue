--This query surfaces people who went through the SUF 2.0 experiment and whether or not they converted
--The experiment is targeted towards anyone who went to the core, blog or paid landing pages
----and then navigated to the sign up page or airkit sign up page within the same session
--We assume that anyone with an email but no lead date subsequent to their page view dropped out of the sign up flow

WITH funnel_pageviews AS (
SELECT pv.user_id,
       session_id,
       CONVERT_TIMEZONE('PST8PDT',pv.time) AS pageview_time,
       path,
       utm_medium,
       CASE WHEN path = '/go/bookkeepers/' AND utm_medium = 'cpc' THEN 'paid'
            WHEN path ILIKE '%blog%' THEN 'blog'
            WHEN path IN ('/' ,
	                        '/how-it-works/',
	                        '/catch-up-bookkeeping-services/',
	                        '/benchretro/',
	                        '/pricing/',
	                        '/small-business-bookkeeping-services/',
	                        '/small-business-tax/',
	                        '/small-business-financial-reports/',
	                        '/small-business-bookkeeping-software/',
	                        '/faq/',
	                        '/customer-reviews/',
	                        '/about/') THEN 'core'
	          WHEN path ILIKE '%signup%' OR title ILIKE 'Bench Funnel 2.0%' THEN 'signup' --the control and variation have different sign up pages
	          END AS bucket,
       MAX(CASE WHEN path ILIKE '%signup%' OR title ILIKE 'Bench Funnel 2.0%' THEN CONVERT_TIMEZONE('PST8PDT',pv.time) END) OVER (PARTITION BY pv.user_id, pv.session_id) AS latest_signup_pageview,
       u.K5y27yvxQCSKw4zmKrxd0w AS core_experiment,
       u.sIIB9YhSSoaqUPMpcwJqiA AS blog_experiment,
       u.kK0HIQ_xR5i_yac3deM3eg AS paid_control_experiment, -- paid control and variation are under different IDs
       u.ED5ku7DoQwaapYloMYKORg AS paid_variation_experiment
FROM {{ source('heap','pageviews') }} pv
LEFT JOIN {{ source('heap','users') }} u ON pv.user_id = u.id
WHERE CONVERT_TIMEZONE('PST8PDT',pv.time) >= '2021-09-01' --experiment start date
  AND bucket IS NOT NULL
),
pageviews_ranked AS (
SELECT fp.*,
       ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY pageview_time DESC) AS rownum
FROM funnel_pageviews fp
WHERE fp.pageview_time <= latest_signup_pageview --implies the viewer must have navigated to a SUP
  AND bucket <> 'signup'
)
SELECT user_id,
       session_id,
       pageview_time,
       h.inferred_email,
       CASE WHEN inferred_email IS NOT NULL AND ld.lead_created_datetime_pstpdt IS NULL THEN 1 ELSE 0 END AS drop_out, --if we have an email but no lead date post page view, then we can assume the person dropped out from the SUF
       CASE WHEN ld.lead_created_datetime_pstpdt >= pageview_time THEN ld.lead_created_datetime_pstpdt END AS lead_datetime_pstpdt,
       CASE WHEN opp.review_call_booked >= DATE_TRUNC('day',pageview_time) THEN opp.review_call_booked END AS trial_date,
       CASE WHEN opp.closedate >= DATE_TRUNC('day',pageview_time) AND stagename = 'Client' THEN opp.closedate END AS client_date,
       bucket,
       core_experiment, -- core placeholder
       blog_experiment, -- blog placeholder
       paid_control_experiment, -- paid control placeholder
       paid_variation_experiment,  -- paid variation placeholder
       sfl.team_suf2__c
FROM pageviews_ranked pv
LEFT JOIN {{ ref('heap_user_identity') }} h ON pv.user_id = h.heap_user_id
LEFT JOIN {{ ref('lead_data') }} ld ON h.inferred_person_id = ld.bench_id__c
LEFT JOIN {{ ref('v_opportunity_data') }} opp ON h.inferred_person_id = opp.bench_id__c
LEFT JOIN {{ source('salesforce','sf_lead') }} sfl ON h.inferred_person_id = sfl.benchid__c
WHERE rownum = 1 --surface the latest core/blog/paid page view prior to viewing a signup page
