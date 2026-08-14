WITH subjects AS (
    SELECT subject, z_score, percentile, value
    FROM exposure_result
    WHERE exposure='%exposure%'
    AND calculation='%calculation%'
    AND set_id='%set_id%'
)

SELECT a.iri, b.percentile, b.name AS name, a.wkb_geometry
FROM {SUBJECT_TABLE} a
JOIN subjects b ON
b.subject=a.iri