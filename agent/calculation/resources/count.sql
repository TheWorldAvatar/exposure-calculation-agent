SELECT COUNT(*) AS intersection_count
FROM {TEMP_TABLE}
WHERE ST_DWithin(
    {TEMP_TABLE}.wkb_geometry,
    ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 3857),
    %(DISTANCE_PLACEHOLDER)s
);
