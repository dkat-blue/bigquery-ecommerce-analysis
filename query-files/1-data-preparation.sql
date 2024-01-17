-- Task 1. Data Preparation for BI Reporting
-- This query extracts key user activity data from the Google Analytics 4 BigQuery obfuscated eCommerce dataset for the year 2021.
-- It focuses on specific eCommerce-related events like session starts, item views, cart actions, and purchases.

SELECT 
  TIMESTAMP_MICROS(e.event_timestamp) AS event_timestamp, -- Converts event timestamps to a readable format.
  e.user_pseudo_id, -- The anonymized user ID.
  (
    -- Retrieves the session ID for each event.
    SELECT ep.value.int_value 
    FROM UNNEST(e.event_params) AS ep 
    WHERE ep.key = 'ga_session_id'
  ) AS session_id,
  e.event_name, -- The name of the event (e.g., session start, view item, etc.).
  e.geo.country AS country, -- The country of the user.
  e.device.category AS device_category, -- The category of the user's device (e.g., mobile, desktop).
  e.traffic_source.source AS source, -- The source of the traffic (e.g., search engine, direct).
  e.traffic_source.medium AS medium, -- The medium of the traffic (e.g., organic, referral).
  e.traffic_source.name AS campaign -- The name of the campaign, if applicable.
FROM 
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
WHERE 
  _TABLE_SUFFIX BETWEEN '20210101' AND '20211231' -- Filters data for events occurring in 2021.
  AND event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')
  -- Filters for specific eCommerce-related events.
;