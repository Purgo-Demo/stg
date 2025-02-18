-- SQL View Creation for HCP Unified Interaction

-- Drop existing view if it exists
DROP VIEW IF EXISTS purgo_playground.hcp_unified_interaction_view;

-- Create a new view for unified HCP interactions
CREATE VIEW purgo_playground.hcp_unified_interaction_view AS
SELECT 
    hcp360.name,
    hcp360.email,
    hcp360.phone,
    interaction.interaction_date,
    interaction.topic,
    interaction.duration_mins,
    interaction.engagement_score,
    -- Calculate is_follow_up based on engagement_score
    CASE WHEN interaction.engagement_score < 3 THEN 1 ELSE 0 END AS is_follow_up,
    -- Calculate follow_up_date only if is_follow_up is TRUE
    CASE WHEN interaction.engagement_score < 3 THEN DATE_ADD(interaction.interaction_date, 30) END AS follow_up_date
FROM 
    purgo_playground.customer_360_raw AS hcp360
INNER JOIN 
    purgo_playground.customer_hcp_interaction AS interaction
    ON (
        CASE 
            -- Join based on phone for In-person channel
            WHEN interaction.channel = 'In-person' THEN hcp360.phone = interaction.phone
            -- Join based on email for other channels
            ELSE hcp360.email = interaction.email
        END
    );

-- Validate View Creation by selecting data from the view
SELECT * FROM purgo_playground.hcp_unified_interaction_view;
