SELECT
    distinct s1.isrc, s1.track, similarity(s1.track, s2.track) AS similarity_score
FROM
    {{ ref('spotify') }} s1
CROSS JOIN
    {{ ref('spotify') }} s2
WHERE
    s1.track != s2.track
    AND similarity(s1.track, s2.track) > 0.8 -- adjust similarity accordingly