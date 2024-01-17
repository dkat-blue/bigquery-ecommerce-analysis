-- Task 2: Conversion Rate Calculation by Date and Traffic Channels
-- This query calculates conversion rates from the start of the session to various stages like cart addition, checkout, and purchase, segmented by date and traffic sources.

WITH cte AS (
  -- Common Table Expression (CTE) to prepare a base dataset of key events and traffic sources
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date, -- Converts event timestamp to date.
    user_pseudo_id || (SELECT value.int_value FROM e.event_params WHERE key = 'ga_session_id') AS user_session_id, -- Concatenates user ID and session ID for a unique session identifier.
    e.event_name,
    e.traffic_source.source AS source, -- Traffic source of the user.
    e.traffic_source.medium AS medium, -- Medium of the traffic.
    e.traffic_source.name AS campaign -- Campaign name, if applicable.
  FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
  WHERE 
    event_name IN ('session_start', 'add_to_cart', 'begin_checkout', 'purchase') -- Filters for specific funnel-related events.
),

ev AS (
  -- Calculate conversion metrics for each stage of the funnel
  SELECT
    sse.event_date,
    sse.source,
    sse.medium,
    sse.campaign,
    COUNT (DISTINCT sse.user_session_id) AS user_session_count, -- Count of unique user sessions.
    COUNT (DISTINCT IF(ae.event_name = 'add_to_cart', sse.user_session_id, NULL)) AS cart_add_count, -- Count of unique sessions leading to cart addition.
    COUNT (DISTINCT IF(ae.event_name = 'begin_checkout', sse.user_session_id, NULL)) AS checkout_count, -- Count of unique sessions leading to checkout.
    COUNT (DISTINCT IF(ae.event_name = 'purchase', sse.user_session_id, NULL)) AS purchase_count -- Count of unique sessions leading to a purchase.
  FROM cte AS sse
  LEFT JOIN cte AS ae
  ON sse.user_session_id = ae.user_session_id AND ae.event_name <> 'session_start'
  WHERE sse.event_name = 'session_start' -- Focuses on sessions starting events.
  GROUP BY 1, 2, 3, 4
)
  
-- Final selection and calculation of conversion rates
SELECT 
  event_date,
  source,
  medium,
  campaign,
  user_session_count,
  cart_add_count,
  ROUND(SAFE_DIVIDE(cart_add_count, user_session_count), 3) AS visit_to_cart, -- Conversion rate from visit to cart addition.
  checkout_count,
  ROUND(SAFE_DIVIDE(checkout_count, user_session_count), 3) AS visit_to_checkout, -- Conversion rate from visit to checkout.
  purchase_count,
  ROUND(SAFE_DIVIDE(purchase_count, user_session_count), 3) AS visit_to_purchase -- Conversion rate from visit to purchase.
FROM 
  ev
ORDER BY 
  1, 2, 3, 4
;