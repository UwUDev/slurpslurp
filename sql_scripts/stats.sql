-- Shows basic db's stats

SELECT
    (SELECT COUNT(id) FROM messages) AS "messages count",
    (SELECT COUNT(id) FROM users) AS "users count"