CREATE TEMP TABLE {TEMP_TABLE} AS
-- AEQD requires geometry to be converted to 4326 first
SELECT ST_Transform(ST_Transform(wkb_geometry, 4326), '{PROJ4_TEXT}')  AS wkb_geometry
FROM {EXPOSURE_DATASET};

CREATE INDEX {TEMP_TABLE}_geom_gix
ON {TEMP_TABLE}
USING GIST (wkb_geometry);