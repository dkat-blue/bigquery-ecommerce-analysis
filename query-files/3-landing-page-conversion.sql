-- Task 3: Landing Page Conversion Comparison
-- This query compares conversion rates across different landing pages for the year 2020, focusing on session start and purchase events.

-- Creating a subquery to identify session starts and their corresponding landing pages
WITH ssp AS (
  SELECT 
    e.user_pseudo_id || (
      -- Combines user ID with session ID for a unique session identifier
      SELECT session_id_param.value.int_value
      FROM UNNEST(e.event_params) AS session_id_param
      WHERE session_id_param.key = 'ga_session_id'
    ) AS user_session_id,
    -- Extracts the path of the page from the page location, defaults to '/' if not found
    COALESCE(
      REGEXP_EXTRACT(
        (
          SELECT page_location_param.value.string_value
          FROM UNNEST(e.event_params) AS page_location_param
          WHERE page_location_param.key = 'page_location'
        ), 
        r'^https?://[^/]+(/[^.?#]*)'
      ),
      '/'
    ) AS page_path
  FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
  WHERE 
    e.event_name = 'session_start'
    AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
),

-- Subquery to identify purchase events
ef AS (
  SELECT   
    e.user_pseudo_id || (
      -- Combines user ID with session ID for a unique session identifier
      SELECT session_id_param.value.int_value
      FROM UNNEST(e.event_params) AS session_id_param
      WHERE session_id_param.key = 'ga_session_id'
    ) AS user_session_id,
    e.event_name
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
  WHERE 
    event_name = 'purchase'
    AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
)

-- Main query to calculate conversion metrics
SELECT
  page_path, -- The path of the landing page.
  COUNT(DISTINCT user_session_id) AS user_session_count, -- Counts unique sessions per landing page.
  SUM(IF(event_name = 'purchase', 1, 0)) AS purchase_count, -- Counts purchases per landing page.
  ROUND(SAFE_DIVIDE(SUM(IF(event_name = 'purchase', 1, 0)), COUNT(DISTINCT user_session_id)), 3) AS visit_to_purchase -- Calculates conversion rate from visit to purchase.
FROM ssp
LEFT JOIN ef USING(user_session_id) -- Joins session starts with purchases based on session ID.
GROUP BY 1; -- Groups results by landing page path.