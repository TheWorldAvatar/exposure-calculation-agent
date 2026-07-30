WITH buffer_circle AS (
    SELECT ST_Buffer(
        ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 3857),
        %(DISTANCE_PLACEHOLDER)s  -- buffer radius in meters
    ) AS geom
),
polygon_union AS (
    SELECT ST_UnaryUnion(ST_Collect(b.wkb_geometry)) AS geom
    FROM {TEMP_TABLE} b
    JOIN buffer_circle c ON
    ST_Intersects(b.wkb_geometry, c.geom)
)

SELECT ST_Area(ST_Intersection(b.geom, c.geom))
FROM buffer_circle c
CROSS JOIN polygon_union b