# Trajectory with time filter (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryTimeFilterCount>` and `<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryTimeFilterCountDetailed>`)

Subject: time series of `org.postgis.Point` instantiated using `com.cmclinnovations.stack.clients.timeseries.TimeSeriesRDBClient` [ref](https://github.com/TheWorldAvatar/stack/blob/main/stack-clients/src/main/java/com/cmclinnovations/stack/clients/timeseries/TimeSeriesRDBClient.java), time class needs to be either `java.time.Instant` or `java.time.ZonedDateTime` (time zone information is necessary to compare trajectories to local opening hours).

Exposure: Vector dataset with an IRI associated with each feature, e.g.

| ogc_fid | wkb_geometry | iri                   |
|--------:|--------------|-----------------------|
| 1       | POINT(1 2)   | <http://feature1>     |
| 2       | POINT(3 4)   | <http://feature2>     |

## Time zone

Time zone information is required to convert local opening hours into timestamps. Time zone data is uploaded as part of the [HD4-stack](https://github.com/TheWorldAvatar/hd4-stack/).

Each time zone is represented by a polygon and a time zone ID, e.g.

```ttl
PREFIX exposure:   <https://www.theworldavatar.com/kg/ontoexposure/>

exposure:timezone/1 a exposure:TimeZone;
    geo:asWKT "POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))"^^geo:wktLiteral;
    exposure:tzid "Europe/London".
```

## Time filtering hierarchy

Time filtering follows the following hierarchy:

1) Dataset level (dcat)
2) Business start and end (ies4)
3) Opening hours (fibo)

### Dataset

Example below shows a dataset instance that is valid for the year 2025:

```text
exposure:dataset1 a dcat:Dataset;
    dcterms:title 'exposure_dataset'
    dcterms:temporal exposure:period;
    exposure:hasIriColumn 'iri'.
exposure:period a dcterms:PeriodOfTime;
    dcat:startDate "2025-01-01"^^xsd:date;
    dcat:endDate "2025-12-31"^^xsd:date.
```

The example below shows a config for the stack-data-uploader, additional triples other than the table name can be added via the "additionalMetadata" property within a dataSubset:

```json
"dataSubsets": [
    {
        "name": "exposure_dataset",
        "type": "vector",
        "additionalMetadata": {
            "prefixes": {
                    "exposure": "https://www.theworldavatar.com/kg/ontoexposure/",
                    "dcterms": "http://purl.org/dc/terms/",
                    "dcat": "http://www.w3.org/ns/dcat#",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
            },
            "triplePatterns": "?dataSubset exposure:hasIriColumn \"iri\"; dcterms:temporal exposure:period. exposure:period a dcterms:PeriodOfTime; dcat:startDate \"2025-01-01\"^^xsd:date; dcat:endDate \"2025-12-31\"^^xsd:date."
        }
    }
]
```

If this information is not present, the dataset is assumed to be valid at all times.

### Business start and end

The temporal information at the dataset level applies to all features within the dataset (table), if it is desired to have more fine-grained start and end times, individual business start and end times can also be added.

The example below shows the start and end times for the instance <http://www.theworldavatar.com/ontology/OntoFHRS/Business/1>.

```text
PREFIX ies4: <http://ies.data.gov.uk/ontology/ies4#>
PREFIX fh: <http://www.theworldavatar.com/ontology/OntoFHRS/>

fh:Business/1 a fh:BusinessEstablishment.

fh:BusinessStart/1 ies4:isStartOf fh:Business/1;
    ies4:inPeriod fh:startperiod/1.

fh:BusinessEnd/1 ies4:isEndOf fh:Business/1;
    ies4:inPeriod fh:endperiod/1.

fh:startperiod/1 a ies4:ParticularPeriod;
    ies4:iso8601PeriodRepresentation "2015-01-01"^^xsd:date.

fh:endperiod/1 a ies4:ParticularPeriod;
    ies4:iso8601PeriodRepresentation "2015-12-31"^^xsd:date.
```

### Opening hours

Each business establishment is allowed to have multiple schedules, example below shows a schedule that repeats everyday:
