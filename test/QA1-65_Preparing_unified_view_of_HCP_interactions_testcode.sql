/* 
  Databricks SQL Test Code for Unified View of HCP Interactions
  This test script verifies the functionality, data integrity, and performance
  of the "hcp_interaction_view" according to specified business rules.
*/

/* Drop the existing view if it exists to ensure a clean environment */
DROP VIEW IF EXISTS purgo_playground.hcp_interaction_view;

/* Create the HCP interaction view to consolidate data based on specifications */
CREATE VIEW purgo_playground.hcp_interaction_view AS
SELECT 
  c.name,
  c.email,
  c.phone,
  h.interaction_date,
  h.topic,
  h.duration_mins,
  h.engagement_score,
  CASE 
    WHEN h.engagement_score < 3 THEN 1 
    ELSE 0 
  END AS is_follow_up,
  CASE 
    WHEN h.engagement_score < 3 THEN DATE_ADD(h.interaction_date, 30) 
    ELSE NULL 
  END AS follow_up_date
FROM
  purgo_playground.customer_360_raw c
INNER JOIN 
  purgo_playground.customer_hcp_interaction h
ON 
  (h.channel = 'In-person' AND c.phone = h.phone) OR 
  (h.channel != 'In-person' AND c.email = h.email);

/* Unit Test: Validate the structure of the view */
WITH view_sample AS (
  SELECT 
    name, 
    email, 
    phone, 
    interaction_date, 
    topic, 
    duration_mins, 
    engagement_score, 
    is_follow_up, 
    follow_up_date 
  FROM 
    purgo_playground.hcp_interaction_view
  LIMIT 1
)
SELECT * FROM view_sample;

/* 
  Expected: The query should return exactly one row with the correct column types:
  name, email, phone as STRING,
  interaction_date as DATE,
  topic as STRING,
  duration_mins as STRING,
  engagement_score as BIGINT,
  is_follow_up as INT,
  follow_up_date as DATE when is_follow_up = 1.
*/

/* Integration Test: Join Consistency */
SELECT COUNT(*) AS num_unmatched_records
FROM purgo_playground.customer_360_raw c
LEFT JOIN purgo_playground.customer_hcp_interaction h
ON ((h.channel = 'In-person' AND c.phone = h.phone) OR (h.channel != 'In-person' AND c.email = h.email))
WHERE h.interaction_id IS NULL;

/*
  Expected: num_unmatched_records should be zero, confirming that the join condition is correctly matching all relevant records.
*/

/* Data Quality Test: Engagement Score Follow-Up */
SELECT 
  COUNT(*) AS incorrect_follow_up
FROM 
  purgo_playground.hcp_interaction_view
WHERE 
  (is_follow_up = 1 AND (engagement_score >= 3 OR follow_up_date IS NULL)) OR
  (is_follow_up = 0 AND follow_up_date IS NOT NULL);

/* 
  Expected: incorrect_follow_up should be zero, indicating that the follow-up logic is accurately applied across records.
*/

/* Performance Test: Check for fast evaluation of total interactions */
SET spark.sql.shuffle.partitions = 10; -- Adjust based on expected data volume

SELECT 
  COUNT(*) AS total_interactions
FROM 
  purgo_playground.hcp_interaction_view;

/*
  Expected: total_interactions should return the count of all joined interactions quickly,
  evaluating view execution efficiency against expected data size.
*/

/* Cleanup: Not required, but practice lightweight operations when possible to keep the environment stable. */
-- DROP VIEW IF EXISTS purgo_playground.hcp_interaction_view;
