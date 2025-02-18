-- Databricks SQL code to drop and create the "hcp_interaction_view",
-- ensuring correct joins based on the provided specifications

/* Drop the existing view if it exists to ensure we start fresh */
DROP VIEW IF EXISTS purgo_playground.hcp_interaction_view;

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
  -- using customer_360_raw as the main source of customer details
  purgo_playground.customer_360_raw c
INNER JOIN 
  -- using customer_hcp_interaction to track all HCP interactions
  purgo_playground.customer_hcp_interaction h
ON 
  -- Join logic: 
  -- "In-person" interactions join on phone
  -- Else, join on email
  (h.channel = 'In-person' AND c.phone = h.phone) OR 
  (h.channel != 'In-person' AND c.email = h.email);

/* 
  Note: 
  - This view aggregates HCP interactions with customer details.
  - Follow-up logic applied where engagement score is less than 3.
  - The interaction data is aligned with the correct customer
    via conditional join logic based on interaction channel.
*/
