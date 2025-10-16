WITH buffer_circle AS (
    SELECT ST_Buffer(
        ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 3857),
        %(DISTANCE_PLACEHOLDER)s  -- buffer radius in meters
    ) AS geom
)

SELECT SUM({TEMP_TABLE}.area * {TEMP_TABLE}.val)
FROM {TEMP_TABLE}, buffer_circle
WHERE ST_Intersects({TEMP_TABLE}.wkb_geometry, buffer_circle.geom)