** Daily Average Purchase Duration (min) vs # of Purchases **
WITH
-- Subquery to find the first time each user visited the site
  first_event AS (
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      MIN(event_timestamp) AS first_event_timestamp
    FROM
      `turing_data_analytics.raw_events`
    GROUP BY
      user_pseudo_id, event_date
    ORDER BY
      user_pseudo_id
  ),
 -- Subquery to find purchase events
  purchase AS(
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
      event_timestamp AS purchase_timestamp
    FROM
    `turing_data_analytics.raw_events`
    WHERE
    event_name='purchase'
  ),
-- Joining the first time and purchase events to calculate duration
  time_to_purchase AS (
    SELECT
      first_event.user_pseudo_id,
      IF(event_date=purchase_date, purchase_timestamp-first_event_timestamp,0) AS duration,
      first_event.event_date
    FROM
      first_event
    JOIN
      purchase
    ON
      first_event.user_pseudo_id=purchase.user_pseudo_id
    ORDER BY
      first_event.user_pseudo_id, first_event.event_date
  )
-- Aggregating and calculating average duration per day
SELECT
    event_date,
    COUNT(user_pseudo_id) AS num_purchase,
    AVG(duration) AS average_duration
FROM
    time_to_purchase
WHERE
    duration != 0
GROUP BY
    event_date
ORDER BY
    event_date;


** Daily Average Purchase Duration (min) vs Revenue **
WITH
-- Subquery to find the first time each user visited the site
  first_event AS (
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      MIN(event_timestamp) AS first_event_timestamp
    FROM
      `turing_data_analytics.raw_events`
    GROUP BY
      user_pseudo_id, event_date
    ORDER BY
      user_pseudo_id
  ),
 -- Subquery to find purchase events
  purchase AS(
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
      event_timestamp AS purchase_timestamp,
      purchase_revenue_in_usd
    FROM
    `turing_data_analytics.raw_events`
    WHERE
    event_name='purchase'
  ),
-- Joining the first time and purchase events to calculate duration
  time_to_purchase AS (
    SELECT
      first_event.user_pseudo_id,
      IF(event_date=purchase_date, purchase_timestamp-first_event_timestamp,0) AS duration,
      first_event.event_date,
      purchase_revenue_in_usd
    FROM
      first_event
    JOIN
      purchase
    ON
      first_event.user_pseudo_id=purchase.user_pseudo_id
    ORDER BY
      first_event.user_pseudo_id, first_event.event_date
  )
-- Aggregating and calculating average duration per day
SELECT
    event_date,
    AVG(duration) AS average_duration,
    SUM(purchase_revenue_in_usd) AS total_revenue,
    AVG(purchase_revenue_in_usd) AS avg_revenue
FROM
    time_to_purchase
WHERE
    duration != 0
GROUP BY
    event_date
ORDER BY
    event_date;


** Average Purchase Duration, Revenue & # of Purchases per Category **
WITH
-- Subquery to find the first time each user visited the site
  first_event AS (
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      MIN(event_timestamp) AS first_event_timestamp
    FROM
      `turing_data_analytics.raw_events`
    GROUP BY
      user_pseudo_id, event_date
    ORDER BY
      user_pseudo_id
  ),
 -- Subquery to find purchase events
  purchase AS(
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
      event_timestamp AS purchase_timestamp,
      purchase_revenue_in_usd,
      category
    FROM
    `turing_data_analytics.raw_events`
    WHERE
    event_name='purchase'
  ),
-- Joining the first time and purchase events to calculate duration
  time_to_purchase AS (
    SELECT
      first_event.user_pseudo_id,
      IF(event_date=purchase_date, purchase_timestamp-first_event_timestamp,0) AS duration,
      first_event.event_date,
      purchase_revenue_in_usd,
      category
    FROM
      first_event
    JOIN
      purchase
    ON
      first_event.user_pseudo_id=purchase.user_pseudo_id
    ORDER BY
      first_event.user_pseudo_id, first_event.event_date
  )
-- Aggregating and calculating average duration per day
SELECT
    event_date,
    AVG(duration) AS average_duration,
    COUNT(user_pseudo_id) AS num_purchase,
    AVG(purchase_revenue_in_usd) AS avg_revenue,
    category
FROM
    time_to_purchase
WHERE
    duration != 0
GROUP BY
    event_date, category
ORDER BY
    event_date;


** Day separated into four blocks: afternoon, evening, night, and morning **
WITH
-- Subquery to find the first time each user visited the site
  first_event AS (
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      MIN(event_timestamp) AS first_event_timestamp
    FROM
      `turing_data_analytics.raw_events`
    GROUP BY
      user_pseudo_id, event_date
  ),
 -- Subquery to find purchase events
  purchase AS (
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
      event_timestamp AS purchase_timestamp,
      purchase_revenue_in_usd
    FROM
      `turing_data_analytics.raw_events`
    WHERE
      event_name='purchase'
  ),
-- Calculate time of day for first event
  first_event_time_of_day AS (
    SELECT
      user_pseudo_id,
      CASE
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(first_event_timestamp)) >= 0 AND EXTRACT(HOUR FROM TIMESTAMP_MICROS(first_event_timestamp)) < 6 THEN 'Night'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(first_event_timestamp)) >= 6 AND EXTRACT(HOUR FROM TIMESTAMP_MICROS(first_event_timestamp)) < 12 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(first_event_timestamp)) >= 12 AND EXTRACT(HOUR FROM TIMESTAMP_MICROS(first_event_timestamp)) < 18 THEN 'Afternoon'
        ELSE 'Evening'
      END AS session_start_time_of_the_day
    FROM
      first_event
  ),
-- Calculate time of day for purchase
  purchase_time_of_day AS (
    SELECT
      user_pseudo_id,
      CASE
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(purchase_timestamp)) >= 0 AND EXTRACT(HOUR FROM TIMESTAMP_MICROS(purchase_timestamp)) < 6 THEN 'Night'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(purchase_timestamp)) >= 6 AND EXTRACT(HOUR FROM TIMESTAMP_MICROS(purchase_timestamp)) < 12 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM TIMESTAMP_MICROS(purchase_timestamp)) >= 12 AND EXTRACT(HOUR FROM TIMESTAMP_MICROS(purchase_timestamp)) < 18 THEN 'Afternoon'
        ELSE 'Evening'
      END AS purchase_time_of_the_day
    FROM
      purchase
  ),
-- Joining the first time and purchase events to calculate duration
  time_to_purchase AS (
    SELECT
      fe.user_pseudo_id,
      IF(fe.event_date = p.purchase_date, p.purchase_timestamp - fe.first_event_timestamp, 0) AS duration,
      fe.event_date,
      p.purchase_revenue_in_usd
    FROM
      first_event fe
    JOIN
      purchase p
    ON
      fe.user_pseudo_id = p.user_pseudo_id
  )
-- Aggregating and calculating average duration per day
SELECT
    event_date,
    session_start_time_of_the_day,
    purchase_time_of_the_day,
    COUNT(time_to_purchase.user_pseudo_id) AS num_purchase,
    AVG(duration) AS average_duration,
    SUM(purchase_revenue_in_usd) AS total_revenue,
    AVG(purchase_revenue_in_usd) AS avg_revenue
FROM
    time_to_purchase
JOIN
    first_event_time_of_day fod
ON
    time_to_purchase.user_pseudo_id = fod.user_pseudo_id
JOIN
    purchase_time_of_day potd
ON
    time_to_purchase.user_pseudo_id = potd.user_pseudo_id
WHERE
    duration != 0
GROUP BY
    event_date, session_start_time_of_the_day, purchase_time_of_the_day
ORDER BY
    event_date;


** Average Purchase Duration, Revenue & # of Purchases per Country **
WITH 
-- Subquery to find the first time each user visited the site
  first_event AS (
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      MIN(event_timestamp) AS first_event_timestamp
    FROM
      `turing_data_analytics.raw_events`
    GROUP BY
      user_pseudo_id, event_date
    ORDER BY
      user_pseudo_id
  ),
 -- Subquery to find purchase events
  purchase AS(
    SELECT
      user_pseudo_id,
      PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
      event_timestamp AS purchase_timestamp,
      purchase_revenue_in_usd,
      country
    FROM
    `turing_data_analytics.raw_events`
    WHERE
    event_name='purchase'
  ),
-- Joining the first time and purchase events to calculate duration
  time_to_purchase AS (
    SELECT
      first_event.user_pseudo_id,
      IF(event_date=purchase_date, purchase_timestamp-first_event_timestamp,0) AS duration,
      first_event.event_date,
      purchase_revenue_in_usd,
      country
    FROM
      first_event
    JOIN
      purchase
    ON
      first_event.user_pseudo_id=purchase.user_pseudo_id
    ORDER BY
      first_event.user_pseudo_id, first_event.event_date
  )
-- Aggregating and calculating average duration per day
SELECT
    event_date,
    AVG(duration) AS average_duration,
    COUNT(user_pseudo_id) AS num_purchase,
    SUM(purchase_revenue_in_usd) AS total_revenue,
    country
FROM
    time_to_purchase
WHERE
    duration != 0
GROUP BY
    event_date, country
ORDER BY
    event_date;
