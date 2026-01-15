WITH buffer AS (
    SELECT ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 4326) AS geom
),
clipped_raster AS (
    SELECT ST_Clip(r.{GEOMETRY_COLUMN}, b.geom) AS clipped, r.{AREA_COLUMN} AS area
    FROM {EXPOSURE_DATASET} r
    CROSS JOIN buffer b
    WHERE ST_Intersects(b.geom, r.{GEOMETRY_COLUMN}) 
    {DATASET_FILTERS}
)
SELECT COALESCE(SUM((ST_SummaryStats(clipped)).sum * area), 0) AS result
FROM clipped_raster