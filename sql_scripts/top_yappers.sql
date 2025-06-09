-- Show the top 10 users who have sent the most messages

SELECT
    author_id,
    COUNT(*) AS message_count
FROM messages
GROUP BY author_id
ORDER BY message_count DESC
LIMIT 10;
