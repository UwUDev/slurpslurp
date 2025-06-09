-- Find the top 10 users who have said a specific term in their messages.
-- You can replace nigg with any term you want to search for.
-- The exemple shows the top 10 racist users in your db

SELECT
    COUNT(m.id) as count,
    u.username,
    COALESCE(u.global_name, u.username) AS display_name,
    u.id
FROM
    messages m
        JOIN
    users u ON m.author_id = u.id
WHERE
    m.content ILIKE '%nigg%'
GROUP BY
    u.id
ORDER BY
    count DESC
LIMIT 10;