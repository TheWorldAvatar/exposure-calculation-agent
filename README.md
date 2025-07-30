# Exposure calculation agent

Calculates exposure of specified subjects to features in the environment.

## Environment variables

1) NAMESPACE (namespace of blazegraph, defaults to kb)
2) DATABASE (database name of postgres, defaults to postgres)

## Note on Ontop usage

Exposure results are instantiated with the full IRIs in the table, this should not be mixed with mappings that make use of `<https://w3id.org/obda/vocabulary#isCanonicalIRIOf>` (particularly the citydb dataset) which is known to cause issues.

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
    derivation:isDerivedFrom <http://exposure>;
    derivation:isDerivedUsing <http://calculation>.
<http://result> a exposure:ExposureResult;
    derivation:belongsTo <http://derivation>;
    exposure:hasValue 123.
```

#### Results for subjects with trajectory

The result instance points to a column in a time series table and it shares the same time series with the trajectory

```ttl
PREFIX derivation: <https://www.theworldavatar.com/kg/ontoderivation/>
PREFIX exposure:   <https://www.theworldavatar.com/kg/ontoexposure/>

<http://derivation> a derivation:Derivation;
    derivation:isDerivedFrom <http://subject>;
    derivation:isDerivedFrom <http://exposure>;
    derivation:isDerivedUsing <http://calculation>.
<http://result> a exposure:ExposureResult;
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
3. `<https://www.theworldavatar.com/kg/ontoexposure/SimpleCount>`
4. `<https://www.theworldavatar.com/kg/ontoexposure/SimpleArea>`
5. `<https://www.theworldavatar.com/kg/ontoexposure/RasterSum>`

Permissible metadata depends on the calculation type. A result instance is instantiated for each subject - exposure - calculation combination.

The choice of projection affects the results greatly. For trajectory based calculations, azimuthal equidistant projection (AEQD) is used, the centroid calculated from the trajectory's envelope. For calculations involving fixed points, EPSG:3857 is used to keep things simple, in case there are points that are far from each other as the AEQD projection relies on a centroid.

#### Trajectory count (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>`)

Overview: Counts features that are within a specified distance from the trajectory using ST_DWithin. [SQL query template here](./agent/calculation/resources/count.sql)

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

#### Simple point count (`<https://www.theworldavatar.com/kg/ontoexposure/SimpleCount>`)

Overview: Counts features that are near each subject using ST_DWithin. [SQL query template here](./agent/calculation/resources/count.sql)

Requirements:
Subject: Any fixed vector with a WKT literal associated via geo:asWKT.
Exposure: Any vector dataset uploaded via the stack data uploader.

The instance of this calculation type:

```ttl
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/SimpleCount>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100.
```

#### Trajectory area (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea>`)

Overview: Calculates intersected area between the buffered trajectory and specified dataset. [SQL query template here](./agent/calculation/resources/area.sql)

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

#### Simple area (`<https://www.theworldavatar.com/kg/ontoexposure/SimpleArea>`)

Overview: Calculates intersected area between a buffered point and polygons in a specified dataset. [SQL query template here](./agent/calculation/resources/area.sql)

Requirements:
Subject: Any fixed vector with a WKT literal associated via geo:asWKT, can be a list of IRI
Exposure: A polygon dataset

```ttl
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/SimpleArea>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100.
```
