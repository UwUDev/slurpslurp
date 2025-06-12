-- Shows basic db's stats

SELECT
    (SELECT COUNT(id) FROM messages) AS "messages count",
    (SELECT COUNT(id) FROM users) AS "users count",
    (SELECT COUNT(id) FROM channels) AS "channels count",
    (SELECT COUNT(id) FROM guilds) AS "guilds count",
    (SELECT COUNT(id) FROM roles) AS "roles count"