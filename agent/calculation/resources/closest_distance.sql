SELECT ST_Distance(ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 3857), {TEMP_TABLE}.wkb_geometry) AS result
FROM {TEMP_TABLE}
ORDER BY
    ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 3857) <-> {TEMP_TABLE}.wkb_geometry
LIMIT 1;