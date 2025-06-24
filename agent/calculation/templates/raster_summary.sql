WITH buffer_circle AS (
    SELECT ST_Buffer(
        ST_Transform(
            ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, %(SRID_PLACEHOLDER)s),
            3857
        ),
        %(DISTANCE_PLACEHOLDER)s  -- buffer radius in meters
    ) AS geom
),
clipped AS (
    SELECT ST_Clip(rast, ST_Transform(buffer_circle.geom, ST_SRID(rast))) AS rast
    FROM buffer_circle, {TABLE_PLACEHOLDER}
    WHERE ST_Intersects(rast, ST_Transform(buffer_circle.geom, ST_SRID(rast)))
)

SELECT (ST_SummaryStats(rast)).*
FROM clipped;
