# Exposure calculation agent

Calculates exposure of specified subjects to features in the environment. This agent is designed to be deployed in the HD4 stack - <https://github.com/TheWorldAvatar/hd4-stack>.

## Environment variables

1) NAMESPACE (namespace of blazegraph, defaults to kb)
2) DATABASE (database name of postgres, defaults to postgres)

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

### Subject

Agent considers two types of subject for exposure calculations: subject with a fixed geometry and subject with a trajectory.

#### Subject with fixed geometry

Agent will query for the WKT literal in the following form:

```ttl
<http://subject> geo:asWKT "POINT(1,2)"^^geo:wktLiteral
```

#### Subject with trajectory

A PostGIS point time series instantiated using the TimeSeriesClient:

```ttl
<http://subject> <https://www.theworldavatar.com/kg/ontotimeseries/hasTimeSeries> <http://timeseries>
```

Time series data:

| time    | points (WKB in database) |
| --------| ------- |
| 1 | POINT(1 2)    |
| 2 | POINT(3 4)    |
| 3 | POINT(5 6)    |

A trajectory can be accompanied by trip data instantiated by the trip agent (<https://github.com/TheWorldAvatar/trip-agent>), the trip data shares the same time series as the subject of exposure:

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
    dcterm:title 'table_name'.
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

The result instance points to a column in a time series table and it shares the same time series with the trajectory

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

If no trip data is present, the entire trajectory is considered as a single trip and a single value is calculated. If trip data is present, a value is calculated for each trip. A new column is added for each subject - exposure - calculation combination. Final results will look something like the following for data with trips. Note that the same value is repeated over each row within a trip.

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

### Calculation

Supported calculation types:

1. `<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>`
2. `<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea>`
3. `<https://www.theworldavatar.com/kg/ontoexposure/Count>`
4. `<https://www.theworldavatar.com/kg/ontoexposure/Area>`
5. `<https://www.theworldavatar.com/kg/ontoexposure/RasterSum>`

Permissible metadata depends on the calculation type. A result instance is instantiated for each subject - exposure - calculation combination.

The choice of projection affects the results greatly. For trajectory based calculations, azimuthal equidistant projection (AEQD) is used, the centroid is calculated from the trajectory's envelope. For calculations involving fixed points, EPSG:3857 is used to keep things simple, in case there are points that are far from each other as the AEQD projection relies on a centroid.

#### Trajectory count (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>`)

Overview: Counts features that are within a specified distance from the trajectory using ST_DWithin. [SQL query template here](./agent/calculation/resources/count_trajectory.sql)

Requirements for subject and exposure dataset:

- Subject: contains a time series of points
- Exposure: Any vector dataset uploaded via the stack data uploader

The instance of this calculation type:

```sparql
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100;
    <https://www.theworldavatar.com/kg/ontoexposure/hasUpperbound> 456;
    <https://www.theworldavatar.com/kg/ontoexposure/hasLowerbound> 123.
```

Distance is mandatory, whereas upper and lower bounds are optional.

#### Count (`<https://www.theworldavatar.com/kg/ontoexposure/Count>`)

Overview: Counts features that are near each subject using ST_DWithin. [SQL query template here](./agent/calculation/resources/count.sql)

Requirements:
Subject: Any fixed vector with a WKT literal associated via geo:asWKT.
Exposure: Any vector dataset uploaded via the stack data uploader.

The instance of this calculation type:

```ttl
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/Count>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100.
```

#### Trajectory area (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea>`)

Overview: Calculates intersected area between the buffered trajectory and specified dataset. [SQL query template here](./agent/calculation/resources/area_trajectory.sql)

Requirements:
Subject: Point time series
Exposure: A polygon dataset

```sparql
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100;
    <https://www.theworldavatar.com/kg/ontoexposure/hasUpperbound> 456;
    <https://www.theworldavatar.com/kg/ontoexposure/hasLowerbound> 123.
```

Distance is mandatory, whereas upper and lower bounds are optional.

#### Area (`<https://www.theworldavatar.com/kg/ontoexposure/Area>`)

Overview: Calculates intersected area between a buffered point and polygons in a specified dataset. [SQL query template here](./agent/calculation/resources/area.sql)

Requirements:
Subject: Any fixed vector with a WKT literal associated via geo:asWKT, can be a list of IRI
Exposure: A polygon dataset

```ttl
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/Area>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100.
```

## User facing APIs

These APIs are not part of the core calculation agent and they are located in [agent\interactor](agent\interactor).

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

2) /csv_export/ (GET)

    Parameters:
    - subject_query_file: SPARQL query template to obtain subject IRIs, bind mounted in the folder called `/app/queries`.
    - subject: IRI of subject
    - subject_label_query_file: SPARQL query template to get user facing label of subject, mandatory SELECT variables - ?Label, ?Feature, where ?Feature is the subject IRIs obtained via `subject_query_file`. A VALUES clause using IRIs from `subject_query_file` is inserted into this query, e.g. VALUES ?Feature {&lt;http://subject1&gt; &lt;http://subject2&gt;}
    - rdf_type: RDF type of calculation
    - exposure_table: table name of exposure dataset

    Example usage:

    ```bash
    curl -o greenspace_2016_raster_area_02.csv 'http://localhost:3838/exposure-calculation-agent/generate_results/?subject_query_file=subject_query.sparql&subject_label_query_file=subject_label_query.sparql&rdf_type=https://www.theworldavatar.com/kg/ontoexposure/RasterArea&exposure_table=ndvi'
    ```

3) /csv_export/trajectory (GET)

    Parameters:
    - subject: IRI of subject
    - rdf_type: RDF type of calculation
    - exposure_table: table name of exposure dataset
    - lowerbound (optional): lowerbound of trajectory time series
    - upperbound (optional): upperbound of trajectory time series

    Example usage:

    ```bash
    curl -o trajectory_result.csv 'http://localhost:3838/exposure-calculation-agent/csv_export/trajectory?rdf_type=https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea&subject=http://trip_trajectory&exposure_table=parks_2016&lowerbound=1715759710072&upperbound=1715759730231'
    ```

## Note on Ontop usage

[agent\calculation\resources\ontop.obda](agent\calculation\resources\ontop.obda) shows some triples that make use of the entire value of a table entry, e.g. `<{subject}>`, instead of something like `derivation:{id}`. When these are mixed together, mappings that make use of `<https://w3id.org/obda/vocabulary#isCanonicalIRIOf>` (Ontop's function to mark two IRIs are equivalent) may not work properly.
