SELECT iri, ST_AsText(wkb_geometry) AS wkt
FROM {TEMP_TABLE}
WHERE ST_DWithin(
    {TEMP_TABLE}.wkb_geometry,
    ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s),
    %(DISTANCE_PLACEHOLDER)s
)
