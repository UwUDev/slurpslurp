-- Find messages before and after a specific message in a channel
-- You'll need to replace '...' with the actual message ID you want to look around.

WITH target_message AS (
    SELECT id, channel_id
    FROM messages
    WHERE id = '...'
),
ordered_messages AS (
    SELECT m.*, ROW_NUMBER() OVER (ORDER BY m.id) as rn
    FROM messages m
    INNER JOIN target_message t ON m.channel_id = t.channel_id
),
target_rn AS (
    SELECT rn
    FROM ordered_messages
    WHERE id = '...'
)
SELECT om.*
FROM ordered_messages om
CROSS JOIN target_rn tr
WHERE om.rn BETWEEN tr.rn - 20 AND tr.rn + 20
ORDER BY om.id;