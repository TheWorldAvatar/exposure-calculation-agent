WITH buffer_circle AS (
    SELECT ST_Buffer(
        ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 3857),
        %(DISTANCE_PLACEHOLDER)s  -- buffer radius in meters
    ) AS geom
)

SELECT SUM(ST_Area(ST_Intersection(wkb_geometry, buffer_circle.geom)))
FROM {TEMP_TABLE}, buffer_circle
WHERE ST_Intersects(wkb_geometry, buffer_circle.geom)