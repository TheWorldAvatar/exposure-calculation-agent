# Exposure calculation agent

Calculates exposure of specified subjects to features in the environment. This agent is designed to be deployed in the HD4 stack - <https://github.com/TheWorldAvatar/hd4-stack>.

## Documentation

- [Core API and data model](#core-api)
- [Calculation reference](#calculation-reference)
- [Shared calculation configuration](#shared-calculation-configuration)
- [User facing APIs](#user-facing-apis)
- [Time filtering](README_time_filter.md)
- [Authenticated Timeline API](README_trajectory_timeline.md)

## Environment variables

1) NAMESPACE (namespace of blazegraph, defaults to kb)
2) DATABASE (database name of postgres, defaults to postgres)
3) VIS_DATA_JSON (file path to data.json for visualisation, only used by routes in agent/interactor/visualisation.py, not needed for calculations)

## Building and debugging

Note that the debugging and production image are separate images.

To build the production image:

```bash
docker compose build
```

To push to the repository:

```bash
docker compose push
```

The stack manager config for production - <https://github.com/TheWorldAvatar/hd4-stack/blob/main/stack-manager/inputs/config/services/exposure-calculation-agent.json>.

To build the debugging image:

```bash
docker compose -f docker-compose-debug.yml build
```

The stack manager config for debugging - <https://github.com/TheWorldAvatar/hd4-stack/blob/main/stack-manager/inputs/config/services/exposure-calculation-agent-debug.json>, debug port is set to 5678.

To attach using VS code, the following config can be added in `launch.json`

```json
{
    "name": "Python: Attach using Debugpy",
    "type": "debugpy",
    "request": "attach",
    "connect": {
        "host": "localhost",
        "port": 5678
    },
    "pathMappings": [
        {
            "localRoot": "${workspaceFolder}",
            "remoteRoot": "/app"
        }
    ]
}
```

## Core API

Route to call the core function:

POST /calculate_exposure with a JSON payload, payload to have the following keys:

- subject: [IRI(s) of subject]
- exposure: [IRI of exposure dataset, added by stack data uploader]
- calculation: [IRI of calculation instance]

Example curl request:

```bash
curl -X POST http://localhost:3838/exposure-calculation-agent/calculate_exposure \
     -H "Content-Type: application/json" \
     -d '{"subject": "http://subject", "exposure": "http://exposure", "calculation": "http://calculation"}'
```

Value of `"subject"` can be a list of IRIs (JSON array) if the subject is not a trajectory, e.g.

```bash
curl -X POST http://localhost:3838/exposure-calculation-agent/calculate_exposure \
     -H "Content-Type: application/json" \
     -d '{"subject": ["http://subject1", "http://subject2"], "exposure": "http://exposure", "calculation": "http://calculation"}'
```

### Query endpoint

This agent uses that stack outgoing federation endpoint <https://github.com/TheWorldAvatar/stack/tree/main/stack-manager#outgoing-stack-endpoint>, please make sure this endpoint is set up correctly, e.g. being able to query the necessary data from here.

### Subject

Agent considers two types of subject for exposure calculations: subject with a fixed geometry and subject with a trajectory.

#### Subject with fixed geometry

Agent will query for the WKT literal in the following form:

```ttl
<http://subject> geo:asWKT "POINT(1 2)"^^geo:wktLiteral
```

#### Subject with trajectory

A PostGIS point time series instantiated using the TimeSeriesClient:

```ttl
<http://subject> <https://www.theworldavatar.com/kg/ontotimeseries/hasTimeSeries> <http://timeseries>
```

Time series data:

| time | points (WKB in database) |
| -------- | ------- |
| 1 | POINT(1 2) |
| 2 | POINT(3 4) |
| 3 | POINT(5 6) |

A trajectory can be accompanied by trip data instantiated by the trip agent (<https://github.com/TheWorldAvatar/trip-agent>), the trip data shares the same time values as the subject of exposure:

| Time |     Point     | Trip |
|------|---------------|------|
| 1    | POINT(1 2)    | 1    |
| 2    | POINT(3 4)    | 1    |
| 3    | POINT(5 6)    | 2    |
| 4    | POINT(7 8)    | 2    |

### Exposure

Points to a table in PostGIS, assumed to be uploaded using the stack data uploader, the following triples should be queryable:

```ttl
<http://dataset> a dcat:Dataset;
    dcterms:title 'table_name'.
```

The dataset type (raster or vector) depends on the calculation type

### Instantiated results

Results instantiated depend on the subject type (trajectory or fixed geometry).

#### Results for subjects with fixed geometry

Results are instantiated in the following form in Ontop:

```ttl
PREFIX derivation: <https://www.theworldavatar.com/kg/ontoderivation/>
PREFIX exposure:   <https://www.theworldavatar.com/kg/ontoexposure/>

<http://derivation> a derivation:Derivation;
    derivation:isDerivedFrom <http://subject>;
    derivation:isDerivedFrom <http://exposure>.
<http://result> a exposure:ExposureResult;
    exposure:hasCalculationMethod <http://calculation>;
    derivation:belongsTo <http://derivation>;
    exposure:hasValue 123;
    exposure:hasUnit "[-]".
```

#### Results for subjects with trajectory

The result instance points to a column in a time series table and it shares the same time series with the trajectory. The trajectory should be instantiated using `com.cmclinnovations.stack.clients.timeseries.TimeSeriesRDBClient`, time series data will be queried from the stack outgoing federated endpoint.

```ttl
PREFIX derivation: <https://www.theworldavatar.com/kg/ontoderivation/>
PREFIX exposure:   <https://www.theworldavatar.com/kg/ontoexposure/>

<http://derivation> a derivation:Derivation;
    derivation:isDerivedFrom <http://subject>;
    derivation:isDerivedFrom <http://exposure>.
<http://result> a exposure:ExposureResult;
    exposure:hasCalculationMethod <http://calculation>;
    exposure:hasUnit "[-]";
    derivation:belongsTo <http://derivation>.
```

If no trip data is present, the entire trajectory is considered as a single trip and a single value is calculated. If trip data is present, a value is calculated for each trip. A new result instance is added for each subject - exposure - calculation combination. Final results will look something like the following for data with trips. Note that the same value is repeated over each row within a trip.

| Time |     Point     | Trip | Result A | Result B |
|------|---------------|------|----------|----------|
| 1    | POINT(1 2)    | 1    | 1        | 2        |
| 2    | POINT(3 4)    | 1    | 1        | 2        |
| 3    | POINT(5 6)    | 2    | 3        | 4        |
| 4    | POINT(7 8)    | 2    | 3        | 4        |

If trip data is not present, entire trajectory is treated as a single trip:

| Time |     Point     | Result A | Result B |
|------|---------------|----------|----------|
| 1    | POINT(1 2)    | 1        | 4        |
| 2    | POINT(3 4)    | 1        | 4        |
| 3    | POINT(5 6)    | 1        | 4        |
| 4    | POINT(5 6)    | 1        | 4        |

## Calculation reference

Types and properties below use the namespace `https://www.theworldavatar.com/kg/ontoexposure/`, abbreviated as `exposure:`. Each subject–dataset–calculation combination has its own result instance.

### Fixed geometry calculations

Subjects have a WKT geometry via `geo:asWKT`. The core API also accepts a list of subject IRIs.

| Type | Exposure dataset | Operation | SQL |
|---|---|---|---|
| `Count` | Vector | Count features within the specified distance | [Query](agent/calculation/resources/count.sql) |
| `Area` | Polygon | Sum intersection area within the subject buffer | [Query](agent/calculation/resources/area.sql) |
| `ClosestDistance` | Vector | Find the closest distance to dataset features | [Query](agent/calculation/resources/closest_distance.sql) |
| `TotalLength` | Line | Sum line length within the subject buffer | [Query](agent/calculation/resources/total_length.sql) |
| `Average` | Vector with values | Average values of nearby features | [Query](agent/calculation/resources/average.sql) |
| `WeightedAverage` | Vector with values and weights | Calculate a weighted average of nearby feature values | [Query](agent/calculation/resources/weighted_average.sql) |
| `Areal` | Vector with values | Assign the value of the intersected feature | [Query](agent/calculation/resources/areal.sql) |
| `RasterSum` | Raster | Sum pixel values within the buffer | [Query](agent/calculation/resources/raster_sum.sql) |
| `RasterAverage` | Raster | Average pixel values within the buffer | [Query](agent/calculation/resources/raster_average.sql) |
| `RasterArea` | Raster with pixel areas | Sum pixel areas within the buffer | [Query](agent/calculation/resources/raster_area.sql) |
| `AreaWeightedSum` | Raster with pixel areas | Sum pixel value × pixel area within the buffer | [Query](agent/calculation/resources/area_weighted_sum_by_raster.sql) |

### Trajectory calculations

Subjects are PostGIS point time series instantiated using `com.cmclinnovations.stack.clients.timeseries.TimeSeriesRDBClient`. Calculations return one value per trip, repeated across the trip's observations. Without trip measures, the selected trajectory is treated as one trip.

| Type | Exposure dataset | Operation | SQL / details |
|---|---|---|---|
| `TrajectoryCount` | Vector | Count features within the specified distance of each trip | [Query](agent/calculation/resources/count_trajectory.sql) |
| `TrajectoryArea` | Polygon | Sum intersection area within each trip buffer | [Query](agent/calculation/resources/area_trajectory.sql) |
| `TrajectoryAreaWeightedSum` | Raster with pixel areas | Sum pixel value × pixel area within each trip buffer | [Query](agent/calculation/resources/area_weighted_sum_trajectory.sql) |
| `TrajectoryRasterArea` | Raster with pixel areas | Sum pixel areas within each trip buffer | [Query](agent/calculation/resources/raster_area_trajectory.sql) |
| `TrajectoryRasterAverage` | Raster | Average pixel values within each trip buffer | [Query](agent/calculation/resources/raster_average_trajectory.sql) |
| `TrajectoryTimeFilterCount` | Vector with feature IRIs and temporal metadata | Count nearby features whose opening hours contain the entire trip | [Time filtering](README_time_filter.md) |
| `TrajectoryTimeFilterCountDetailed` | Vector with feature IRIs and temporal metadata | Allow trip overlap, then check the timestamp of the closest recorded GPS point against opening hours | [Time filtering](README_time_filter.md) |

For authenticated calculations across a user's devices, see the [Timeline API](README_trajectory_timeline.md).

## Shared calculation configuration

### Calculation instance

Use `hasDistance` for buffer/proximity calculations, in metres. Trajectory calculations also accept optional `hasLowerbound` and `hasUpperbound` to select observations.

```turtle
@prefix exposure: <https://www.theworldavatar.com/kg/ontoexposure/> .

<http://calculation> a exposure:TrajectoryCount;
    exposure:hasDistance 100;
    exposure:hasLowerbound 123;
    exposure:hasUpperbound 456.
```

For a fixed subject, change the type (for example, to `exposure:Count`) and omit the trajectory bounds. Bound formats depend on the calling API; the [Timeline API](README_trajectory_timeline.md) requires ISO datetimes with timezones and does not accept numeric epoch bounds.

### Dataset columns

Datasets must be uploaded through the stack data uploader and located in the same stack. Geometry columns default to `wkb_geometry` for vectors and `rast` for rasters. Override them on the dataset:

```turtle
@prefix exposure: <https://www.theworldavatar.com/kg/ontoexposure/> .

<http://exposure> exposure:hasGeometryColumn "custom_geometry".
```

`Average`, `WeightedAverage`, and `Areal` require a value column on the calculation:

```turtle
@prefix exposure: <https://www.theworldavatar.com/kg/ontoexposure/> .

<http://calculation> a exposure:Areal;
    exposure:hasColumnName "value_column".
```

### Raster area metadata

`AreaWeightedSum`, `RasterArea`, `TrajectoryAreaWeightedSum`, and `TrajectoryRasterArea` require a precomputed **pixel area** for each raster tile. The area column defaults to `area`; specify `hasAreaColumn` if it has another name. This value represents the area of one pixel in the tile, rather than the total tile area.

```turtle
@prefix exposure: <https://www.theworldavatar.com/kg/ontoexposure/> .

<http://exposure> a exposure:AreaWeightedDataset;
    exposure:hasAreaColumn "area";
    exposure:hasGeometryColumn "rast".

<http://calculation> a exposure:TrajectoryRasterArea;
    exposure:hasDistance 100.
```

Use pixel areas in square metres for area results in m². `TrajectoryRasterAverage` requires no area column; configure it with the same distance metadata and a raster dataset.

### Raster trajectory behaviour

The three raster trajectory calculations clip tiles to each trip buffer and exclude NoData pixels from summary statistics:

- `TrajectoryAreaWeightedSum`: sum each tile's retained pixel values, multiply by its pixel area, then sum across tiles.
- `TrajectoryRasterArea`: multiply each tile's retained pixel count by its pixel area, then sum across tiles.
- `TrajectoryRasterAverage`: divide the total retained pixel sum by the total retained pixel count across tiles.

They return zero when no valid pixels contribute. All tiles selected by the dataset filters must share one known, positive SRID. Tiles remain in their original CRS; the agent transforms each AEQD trip buffer into that CRS before clipping.

### Dataset filters

Attach filters to the calculation to restrict dataset rows. Multiple filters are combined, for example `year = 2000`:

```turtle
@prefix exposure: <https://www.theworldavatar.com/kg/ontoexposure/> .

<http://calculation> a exposure:Area;
    exposure:hasDistance 100;
    exposure:hasDatasetFilter <http://dataset_filter>.

<http://dataset_filter> exposure:hasFilterColumn "year";
    exposure:hasFilterValue 2000.
```

### Coordinate systems

Projection choice affects distances and areas. Fixed geometry calculations use EPSG:3857. Trajectory calculations use a local azimuthal equidistant (AEQD) projection centred on the centroid of the trajectory's envelope. Raster trajectory calculations construct buffers in AEQD and transform them to the raster CRS before clipping.

## User facing APIs

These APIs are not part of the core calculation agent and they are located in [agent/interactor](agent/interactor).

The following APIs are used to initialise the necessary instances and trigger the core agent.

1) /trigger_calculation/ (POST)

    Parameters:
    - subject_query_file: SPARQL query template to obtain subject IRIs, bind mounted in the folder called `/app/queries`. The result of this query should give IRI(s) of subject that we wish to calculate for. The query must have one SELECT parameter and can take any name.
    - subject: IRI of subject to calculate for
    - rdf_type: RDF type of calculation to perform
    - distance: buffer distance for calculation
    - exposure_table: table name of exposure dataset (needs to be added via the stack data uploader to ensure the necessary triples are present)
    - upperbound (optional): optional upperbound for trajectory calculations
    - lowerbound (optional): optional lowerbound for trajectory calculations

    Either `subject_query_file` or `subject` needs to be provided in the request.
    Example usage:

    ```bash
    curl -X POST http://localhost:3838/exposure-calculation-agent/trigger_calculation/?subject_query_file=subject_query.sparql&rdf_type=https://www.theworldavatar.com/kg/ontoexposure/Count&distance=400&exposure_table=parks
    ```

    Overview:
    1) This route checks whether a calculation instance with the specified RDF type and metadata (e.g. distance) exists, then instantiate one if necessary.
    2) If `subject_query_file` is given, it will run the query to obtain the subject IRIs, otherwise IRI is simply obtained from the `subject` parameter.
    3) Then it queries the dataset IRI of the given `exposure_table`, because the core agent is designed to read in IRIs only.
    4) Finally sends a request to the core agent with the IRIs of subject, exposure, and calculation.

2) /csv_export/non_trajectory (POST)

    No parameters, instead inputs to be provided in the request body as JSON, e.g.

   ```bash
    curl -X POST "http://localhost:3838/exposure-calculation-agent/csv_export/non_trajectory" -H "Content-Type: application/json" -d @body.json
    ```

    where the content of **body.json** is something like

    ```json
    {
        "exposure_table": "ndvi_raster",
        "rdf_type": "https://www.theworldavatar.com/kg/ontoexposure/AreaWeightedSum",
        "dataset_filter_values": {
            "year": [
                2016
            ]
        },
        // provide either subject_query_file or subject, not both
        "subject_query_file": "subject_query.sparql",
        "subject_label_query_file": "subject_label_query.sparql"
    }
    ```

    Response is a csv file.

3) /csv_export/trajectory (GET)

    Parameters:
    - `subject`: IRI of subject
    - `rdf_type`: RDF type of calculation
    - `exposure_table`: table name of exposure dataset
    - `lowerbound` (optional): lowerbound of trajectory time series
    - `upperbound` (optional): upperbound of trajectory time series
    - `include_lat_lng` (optional, default true): include geolocation in exported CSV file
    - `refresh_of_cache` (optional, default false): refresh outgoing federation cache before querying

    Example usage:

    ```bash
    curl -o trajectory_result.csv 'http://localhost:3838/exposure-calculation-agent/csv_export/trajectory?rdf_type=https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea&subject=http://trip_trajectory&exposure_table=parks_2016&lowerbound=1715759710072&upperbound=1715759730231'
    ```

4) /trigger_calculation/bulk (POST)

   No parameters, instead inputs to be provided in the request body as JSON, e.g.

   ```bash
    curl -X POST "http://localhost:3838/exposure-calculation-agent/trigger_calculation/bulk" -H "Content-Type: application/json" -d @body.json
    ```

    where the content of **body.json** is something like

    ```json
    {
        "exposure_table": "ndvi_raster",
        "rdf_types": [
            "https://www.theworldavatar.com/kg/ontoexposure/AreaWeightedSum"
        ],
        "distances": [
            400,
            800,
            1000
        ],
        "dataset_filter_values": {
            "year": [
            2016
            ]
        },
        // provide either subject_query_file or subject, not both
        "subject_query_file": "subject_query.sparql",
        "subject": "http://subject"
    }
    ```

    dataset_filter_values is optional. A cross product between the provided distances and dataset filters is done to produce all combinations of parameters, then calculations are executed for each of the combinations.
