-- Task 3 (Alternate Approach): Landing Page Conversion Comparison
-- This query also compares conversion rates across different landing pages for the year 2020. 
-- Unlike the previous approach, this one uses a window function to determine purchases within the same query structure.

-- Subquery to gather session data and identify if a purchase was made in the session
WITH session_data AS (
  SELECT 
    e.user_pseudo_id || (
      -- Combines user ID with session ID for a unique session identifier
      SELECT value.int_value 
      FROM UNNEST(e.event_params) 
      WHERE key = 'ga_session_id'
    ) AS user_session_id,
    -- Extracts the path of the page from the page location, defaults to '/' if not found
    COALESCE(
      REGEXP_EXTRACT(
        (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location'), 
        r'^https?://[^/]+(/[^.?#]*)'
      ),
      '/'
    ) AS page_path,
    -- Uses a window function to flag if a purchase was made during the session
    MAX(IF(e.event_name = 'purchase', 1, 0)) OVER (PARTITION BY e.user_pseudo_id, (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id')) AS made_purchase
  FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
  WHERE 
    e.event_name IN ('session_start', 'purchase')
    AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
)

-- Main query to calculate conversion metrics
SELECT
  page_path, -- The path of the landing page.
  COUNT(DISTINCT user_session_id) AS user_session_count, -- Counts unique sessions per landing page.
  SUM(made_purchase) AS purchase_count, -- Sums the made_purchase flags to count purchases per landing page.
  ROUND(SAFE_DIVIDE(SUM(made_purchase), COUNT(DISTINCT user_session_id)), 3) AS visit_to_purchase -- Calculates conversion rate from visit to purchase.
FROM 
  session_data
GROUP BY 1
ORDER BY 4 DESC; -- Orders by conversion rate in descending order.
