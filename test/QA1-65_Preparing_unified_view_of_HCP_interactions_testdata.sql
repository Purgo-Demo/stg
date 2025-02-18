-- Dropping the existing view if it exists
DROP VIEW IF EXISTS purgo_playground.hcp_interaction_view;

-- Create a new view for HCP interactions
CREATE VIEW purgo_playground.hcp_interaction_view AS
SELECT 
  c.name,  -- Customer's name from customer_360_raw
  c.email, -- Customer's email from customer_360_raw
  c.phone, -- Customer's phone from customer_360_raw
  h.interaction_date, -- Interaction date from customer_hcp_interaction
  h.topic, -- Interaction topic from customer_hcp_interaction
  h.duration_mins, -- Interaction duration in minutes from customer_hcp_interaction
  h.engagement_score, -- Engagement score from customer_hcp_interaction

  -- Determine if follow-up is needed based on engagement score
  CASE 
    WHEN h.engagement_score < 3 THEN 1 
    ELSE 0 
  END AS is_follow_up,

  -- Calculate follow-up date if is_follow_up is required
  CASE 
    WHEN h.engagement_score < 3 THEN date_add(h.interaction_date, 30) 
    ELSE NULL 
  END AS follow_up_date

FROM
  purgo_playground.customer_360_raw c
INNER JOIN 
  purgo_playground.customer_hcp_interaction h
ON 
  (h.channel = 'In-person' AND c.phone = h.phone) -- Join on phone for 'In-person' channel
  OR (h.channel != 'In-person' AND c.email = h.email); -- Join on email for other channels

-- Note:
-- 1. This view aggregates and joins customer_360_raw and customer_hcp_interaction based on provided specifications.
-- 2. is_follow_up is flagged if engagement_score is below 3, indicating potential follow-up requirements.
-- 3. follow_up_date is computed to be 30 days post the interaction date when follow-up is necessary.
