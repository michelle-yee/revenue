-- =============================================================================
-- This view consolidates daily in-platform metrics from Facebook, Google and Bing
-- The data is on an ad level, and can be rolled up by ad group or campaign

-- Note: Facebook in-platform lead conversions may differ slightly from Facebook reports
----because we only look for the offsite_conversion.fb_pixel_lead action type
-- =============================================================================


--Facebook ads
WITH fb_leads AS
(
SELECT
	_sdc_source_key_date_start AS report_date,
	_sdc_source_key_ad_id AS ad_id,
	value AS platform_leads
FROM {{ source('facebookads','ads_insights__actions') }}
WHERE action_type = 'offsite_conversion.fb_pixel_lead' -- This is what we currently use to determine an in-platform FB lead conversion. It does not include inquiry conversions.
),

--Bing ad data
--This CTE grabs the latest report data since Stitch syncs copies of the report into the table without deleting the old report
bing_ads AS
(
SELECT *
  FROM (
SELECT *,
       RANK() OVER (PARTITION BY timeperiod, adid, adgroupname, campaignname
                    ORDER BY _sdc_report_datetime DESC)
FROM {{ source('bingads','ad_performance_report') }}
ORDER BY timeperiod DESC
       ) AS latest
 WHERE latest.rank = 1
),

--Google ad data
--This CTE grabs the latest report data since Stitch syncs copies of the report into the table without deleting the old report
google_ads AS
(
SELECT *
  FROM (
        SELECT *,
        RANK() OVER (PARTITION BY day, adid, adgroupid, campaignid
                     ORDER BY _sdc_report_datetime DESC)
        FROM {{ source('googleads','ad_performance_report') }}
        ORDER BY day ASC
       ) AS latest
 WHERE latest.rank = 1
 ),

--Google campaign data
--This CTE grabs the latest report data since Stitch syncs copies of the report into the table without deleting the old report
--We need data specific from the googleads.campaign_performance_report since the search impression share data is not available at the ad grain
google_campaigns AS
(
SELECT *
  FROM (
        SELECT *,
        RANK() OVER (PARTITION BY day, campaignid
                     ORDER BY _sdc_report_datetime DESC)
        FROM {{ source('googleads','campaign_performance_report') }}
        ORDER BY day ASC
       ) AS latest
 WHERE latest.rank = 1
 )

SELECT
	date_start AS report_date,
	'facebook' AS platform,
	campaign_id::BIGINT,
	campaign_name,
	adset_id::BIGINT,
	adset_name,
	fb.ad_id::BIGINT,
	ad_name,
	spend,
	reach, -- fb specific metric
	impressions,
	NULL AS elig_impressions,
	clicks,
	inline_link_clicks, -- fb specific metric
	fb_leads.platform_leads,
	0 AS searchimprshare_valid
FROM {{ source('facebookads','ads_insights') }} fb
LEFT JOIN fb_leads
	ON fb.ad_id = fb_leads.ad_id
	AND fb.date_start = fb_leads.report_date

UNION

--Bing ads
SELECT
	timeperiod AS report_date,
	'bing' AS platform,
	campaignid AS campaign_id,
	campaignname AS campaign_name,
	adgroupid AS adgroup_id,
	adgroupname AS adgroup_name,
	adid AS ad_id,
	NULL AS ad_name,
	spend,
	NULL AS reach,
	impressions,
	NULL AS elig_impressions,
	clicks,
	NULL AS inline_link_clicks,
	conversions AS platform_leads,
	0 AS searchimprshare_valid
FROM bing_ads

UNION ALL

--Google ads
SELECT
	g.day AS report_date,
	'google' AS platform,
	g.campaignid AS campaign_id,
	g.campaign AS campaign_name,
	g.adgroupid AS adgroup_id,
	g.adgroup AS adgroup_name,
	g.adid AS ad_id,
	g.ad AS ad_name,
	g.cost/1000000.0 AS spend,
	NULL AS reach,
	g.impressions,
	NULL AS elig_impressions,
	g.clicks,
	NULL AS inline_link_clicks,
	g.conversions AS platform_lead,
	CASE WHEN c.searchimprshare IS NOT NULL THEN 1 ELSE 0 END AS searchimprshare_valid
FROM google_ads g
LEFT JOIN google_campaigns c
	ON g.campaign = c.campaign
	AND g.day = c.day
	AND c.searchimprshare IS NOT NULL

UNION ALL

--Google campaign data for the sole purpose of surfacing eligible impressions
--We can calculate search impression share using eligible impressions in downstream dashboards
SELECT
	c.day AS report_date,
	'google' AS platform,
	campaignid AS campaign_id,
	campaign AS campaign_name,
	NULL AS adgroup_id,
	NULL AS adgroup_name,
	NULL AS ad_id,
	NULL AS ad_name,
	NULL AS spend,
	NULL AS reach,
	NULL impressions,
	impressions/(searchimprshare/100) AS elig_impressions,
	NULL AS clicks,
	NULL AS inline_link_clicks,
	NULL AS platform_leads,
	1 AS searchimprshare_valid
FROM google_campaigns c
WHERE searchimprshare IS NOT NULL
