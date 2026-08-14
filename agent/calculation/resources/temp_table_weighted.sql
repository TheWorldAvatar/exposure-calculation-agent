-- this requires an additional column to obtain value from
CREATE TEMP TABLE {TEMP_TABLE} AS
SELECT ST_Transform("{GEOMETRY_COLUMN}", 3857) AS wkb_geometry, "{COLUMN_NAME}" AS value, "{WEIGHT_COLUMN}" AS weight 
FROM {EXPOSURE_DATASET}
{DATASET_FILTERS};

CREATE INDEX {TEMP_TABLE}_geom_gix
ON {TEMP_TABLE}
USING GIST (wkb_geometry);