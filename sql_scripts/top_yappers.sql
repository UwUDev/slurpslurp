-- Show the top 10 users who have sent the most messages

SELECT
    COUNT(m.id) AS message_count,
    u.username,
    COALESCE(u.global_name, u.username) AS display_name,
    u.id
FROM users u
         JOIN messages m ON u.id = m.author_id
GROUP BY u.id, u.username, u.global_name
ORDER BY message_count DESC
LIMIT 10;
