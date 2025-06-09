-- Shows basic db's stats

SELECT
    (SELECT COUNT(id) FROM messages) AS "messages count"