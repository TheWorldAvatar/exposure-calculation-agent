WITH buffer AS (
    SELECT ST_Transform(
        ST_Buffer(ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s), %(DISTANCE_PLACEHOLDER)s),
        %(PROJ4_TEXT)s,
        %(RASTER_SRID)s
    ) AS geom
),
clipped_raster AS (
    SELECT ST_Clip(r.{GEOMETRY_COLUMN}, b.geom) AS clipped,
           r.{AREA_COLUMN} AS area
    FROM {EXPOSURE_DATASET} r
    CROSS JOIN buffer b
    WHERE ST_ConvexHull(r.{GEOMETRY_COLUMN}) && b.geom
      AND ST_Intersects(b.geom, r.{GEOMETRY_COLUMN})
    {DATASET_FILTERS}
)
SELECT COALESCE(SUM((ST_SummaryStats(clipped)).count * area), 0) AS exposure_result
FROM clipped_raster;
