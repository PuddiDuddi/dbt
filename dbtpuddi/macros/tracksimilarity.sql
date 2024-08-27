{% test tracksimilarity(model, column_name) %}

    {{ config(serverity = 'warn') }}

    WITH tracks AS (
        SELECT DISTINCT {{ column_name }} FROM {{ model }}
    )
    SELECT
        t1.{{ column_name }} AS original_track,
        t2.{{ column_name }} AS similar_track,
        similarity(t1.{{ column_name }}, t2.{{ column_name }}) AS similarity_score
    FROM
        tracks t1
    CROSS JOIN
        tracks t2
    WHERE
        t1.{{ column_name }} != t2.{{ column_name }}
        AND similarity(t1.{{ column_name }}, t2.{{ column_name }}) > 0.8 -- adjust similarity accordingly
    ORDER BY
        t1.{{ column_name }},
        similarity_score DESC
{% endtest %}