# Exposure calculation agent

Calculates exposure of specified subjects to features in the environment

Assumptions:

1) Point time series table as SRID assigned
2) Exposure datasets are uploaded using default GDAL settings with the stack data uploader.

## Environment variables

1) NAMESPACE (namespace of blazegraph, defaults to kb)
2) DATABASE (database name of postgres, defaults to postgres)

## Note on Ontop usage

Exposure results are instantiated with the full IRIs in the table, this should not be mixed with mappings that make use of <https://w3id.org/obda/vocabulary#isCanonicalIRIOf> (particularly the citydb dataset) which is known to cause issues.

## Core API

Route to call the core function:
POST /calculate_exposure with a JSON payload, payload to have the following keys:

- subject [IRI(s) of subject]
- exposure [IRI of exposure dataset, added by stack data uploader]
- calculation [IRI of calculation instance]

If spun up using the given stack manager config in [./stack-manager/](./stack-manager/)

```bash
curl -X POST http://localhost:3838/exposure-calculation-agent/calculate_exposure \
     -H "Content-Type: application/json" \
     -d '{"subject": "http://subject", "exposure": "http://exposure", "calculation": "http://calculation"}'
```

### Subject

Broadly separated into subjects with a fixed geometry and trajectories.

Subjects with a fixed geometry:

```ttl
<http://subject> geo:asWKT "POINT(1,2)"^^geo:wktLiteral
```

Subjects with a trajectory, instantiated using the TimeSeriesClient:

```ttl
<http://subject> <https://www.theworldavatar.com/kg/ontotimeseries/hasTimeSeries> <http://timeseries>
```

Time series data:

| time    | points (displayed as WKB in database) |
| --------| -------|
| 1 | POINT(1 2)    |
| 2 | POINT(3 4)    |
| 3 | POINT(5 6)    |

### Exposure

Points to a table in PostGIS, assumed to be uploaded using the stack data uploader, the following triples should be queryable:

```ttl
<http://dataset> a dcat:Dataset;
    dcterm:title 'table_name'.
```

These are always in the `kb` namespace regardless of the `NAMESPACE` parameter because they are added by the stack data uploader.

### Calculation

Supported calculation types:

1. `<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>`
2. `<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea>`
3. `<https://www.theworldavatar.com/kg/ontoexposure/SimpleCount>`
4. `<https://www.theworldavatar.com/kg/ontoexposure/SimpleArea>`

Permissible metadata depends on the calculation type.

#### Trajectory count (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>`)

Counts features that are close to a given trajectory.

Mandatory property:

- distance

optional properties for time series query, format of literal depends on the instantiated time series (e.g. Instant, epoch):

- upperbound
- lowerbound

The instance of this calculation type:

```sparql
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100;
    <https://www.theworldavatar.com/kg/ontoexposure/hasUpperbound> 456;
    <https://www.theworldavatar.com/kg/ontoexposure/hasLowerbound> 123.
```

Optional trip data instantiated by trip agent (<https://github.com/TheWorldAvatar/trip-agent>):



Instantiated results:

Time series data and some triples will be added, a derivation instance is created for each subject - exposure - calculation pair, i.e.

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

#### Simple count (`<https://www.theworldavatar.com/kg/ontoexposure/SimpleCount>`)

Counts features that are near specified geometries.

Provided subject URL needs to have a WKT literal attached as a triple, i.e.

```ttl
<subject> geo:asWKT "POINT(1,2)"^^geo:wktLiteral
```

Will federate across specified Blazegraph NAMESPACE and default Ontop SPARQL endpoint (the Ontop container without dataset name appended).

The instance of this calculation type:

```sparql
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 100.
```

#### Trajectory area (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea>`)

#### Simple area (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryArea>`)