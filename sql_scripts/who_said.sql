-- Find the top 10 users who have said a specific term in their messages.
-- You can replace nigg with any term you want to search for.
-- The exemple shows the top 10 racist users in your db

SELECT
    COUNT(m.id) AS racism_score,
    m.author_id
FROM
    messages m
WHERE
    m.content ILIKE '%nigg%'
GROUP BY
    m.author_id
ORDER BY
    racism_score DESC
LIMIT 10;