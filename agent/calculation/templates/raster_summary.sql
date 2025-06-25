WITH buffer_circle AS (
    SELECT ST_Buffer(
        ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 3857),
        %(DISTANCE_PLACEHOLDER)s  -- buffer radius in meters
    ) AS geom
),
clipped AS (
    SELECT ST_Clip(rast, buffer_circle.geom) AS rast
    FROM buffer_circle, {TEMP_TABLE}
    WHERE ST_Intersects(rast, buffer_circle.geom)
)

SELECT (ST_SummaryStats(rast)).*
FROM clipped;
