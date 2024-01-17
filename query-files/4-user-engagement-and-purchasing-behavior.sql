-- Task 4: Correlation Analysis between User Engagement and Purchases
-- This query aims to determine the correlation between user engagement (both in terms of engagement status and time) and the occurrence of purchases within a session.

-- Subquery to gather engagement data and purchase event for each session
WITH events AS (
  SELECT 
    e.user_pseudo_id || (
      -- Combines user ID with session ID for a unique session identifier
      SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id'
    ) AS user_session_id,
    -- Determines if the user was engaged in the session (1 for engaged, 0 for not)
    IF((SELECT COALESCE(value.int_value, SAFE_CAST(value.string_value AS INT64)) FROM UNNEST(e.event_params) WHERE key = 'session_engaged') = 1, 1, 0) AS user_engaged,
    event_name,
    -- Sums up engagement time for the session, defaulting to 0 if not available
    COALESCE((SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'engagement_time_msec'), 0) AS engagement_time
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
),

-- Aggregates the engagement data and purchase information per session
aggregated_data AS (
  SELECT 
    user_session_id,
    MAX(user_engaged) user_engaged, -- Flags if the user was engaged at any point in the session
    SUM(engagement_time) engagement_time, -- Total engagement time per session
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase_made -- Flags if a purchase was made in the session
  FROM events
  GROUP BY user_session_id
)

-- Calculates correlation coefficients between engagement metrics and purchase occurrence
SELECT 
  ROUND(CORR(user_engaged, purchase_made), 3) AS eng_purchase_corr, -- Correlation between user engagement status and purchase
  ROUND(CORR(engagement_time, purchase_made), 3) AS eng_time_purchase_corr -- Correlation between total engagement time and purchase
FROM aggregated_data;