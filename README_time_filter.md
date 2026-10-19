# Trajectory with time filter (`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryTimeFilterCount>` and `<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryTimeFilterCountDetailed>`)

Subject: time series of `org.postgis.Point` instantiated using `com.cmclinnovations.stack.clients.timeseries.TimeSeriesRDBClient` [ref](https://github.com/TheWorldAvatar/stack/blob/main/stack-clients/src/main/java/com/cmclinnovations/stack/clients/timeseries/TimeSeriesRDBClient.java), time class needs to be either `java.time.Instant` or `java.time.ZonedDateTime` (time zone information is necessary to compare trajectories to local opening hours).

Exposure: Vector dataset with an IRI associated with each feature, e.g.

| ogc_fid | wkb_geometry | iri                   |
|--------:|--------------|-----------------------|
| 1       | POINT(1 2)   | <http://feature1>     |
| 2       | POINT(3 4)   | <http://feature2>     |

Calculation instance:

```ttl
<http://calculation> a <https://www.theworldavatar.com/kg/ontoexposure/TrajectoryTimeFilterCount>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasDistance> 50.
```

Each feature needs an IRI. The IRI column defaults to `iri` and the geometry column to `wkb_geometry`; specify these properties to override the defaults:

```ttl
<http://exposure> a <https://www.theworldavatar.com/kg/ontoexposure/ExposureDataset>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasIriColumn> "iri";
    <https://www.theworldavatar.com/kg/ontoexposure/hasGeometryColumn> "wkb_geometry".
```

## Time zone

Time zone information is required to compare trajectory timestamps with local opening hours. The agent selects one time zone using the centroid of the trajectory envelope; it does not select a separate zone for each feature or trip. Time zone data is uploaded as part of the [HD4-stack](https://github.com/TheWorldAvatar/hd4-stack/).

Each time zone is represented by a polygon and a time zone ID, e.g.

```ttl
PREFIX exposure:   <https://www.theworldavatar.com/kg/ontoexposure/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

<https://www.theworldavatar.com/kg/ontoexposure/timezone/1> a exposure:TimeZone;
    geo:asWKT "POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))"^^geo:wktLiteral;
    exposure:tzid "Europe/London".
```

## Workflow

1) Geospatial filtering
   1) Query trajectory via federated SPARQL
   2) Construct a line for each trip
   3) Collect IRIs of intersected features for each trip (via SQL)
2) Time filtering
   1) Query business start/end and opening hours of each intersected feature (via SPARQL)
   2) Do time filtering in memory (python), refer to [time filtering hierarchy](#time-filtering-hierarchy) section for more details

## Time filtering hierarchy

Time filtering follows the following hierarchy:

1) Dataset level (dcat)
2) Business start and end (ies4)
3) Opening hours (fibo)

### Dataset

Example below shows a dataset instance that is valid for the year 2025:

```turtle
PREFIX exposure: <https://www.theworldavatar.com/kg/ontoexposure/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

exposure:dataset1 a dcat:Dataset;
    dcterms:title 'exposure_dataset';
    dcterms:temporal exposure:period;
    exposure:hasIriColumn 'iri'.
exposure:period a dcterms:PeriodOfTime;
    dcat:startDate "2025-01-01"^^xsd:date;
    dcat:endDate "2025-12-31"^^xsd:date.
```

The example below shows a config for the stack-data-uploader, additional triples other than the table name can be added via the "additionalMetadata" property within a dataSubset:

```json
{
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
}
```

Both dataset start and end dates must be present for this restriction to apply; otherwise it is ignored. When present, the entire trip must fall within the inclusive local date range for either calculation type.

### Business start and end

The temporal information at the dataset level applies to all features within the dataset (table), if it is desired to have more fine-grained start and end times, individual business start and end times can also be added.

Both business start and end must be present for the restriction to apply. Without a complete pair, the business is assumed to exist at all times. Both calculation types require the entire trip to fit within the business lifetime.

The example below shows the start and end times for the instance `<http://www.theworldavatar.com/ontology/OntoFHRS/Business/1>`.

```turtle
PREFIX ies4: <http://ies.data.gov.uk/ontology/ies4#>
PREFIX fh: <http://www.theworldavatar.com/ontology/OntoFHRS/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

<http://www.theworldavatar.com/ontology/OntoFHRS/Business/1> a fh:BusinessEstablishment.

<http://www.theworldavatar.com/ontology/OntoFHRS/BusinessStart/1> ies4:isStartOf <http://www.theworldavatar.com/ontology/OntoFHRS/Business/1>;
    ies4:inPeriod <http://www.theworldavatar.com/ontology/OntoFHRS/startperiod/1>.

<http://www.theworldavatar.com/ontology/OntoFHRS/BusinessEnd/1> ies4:isEndOf <http://www.theworldavatar.com/ontology/OntoFHRS/Business/1>;
    ies4:inPeriod <http://www.theworldavatar.com/ontology/OntoFHRS/endperiod/1>.

<http://www.theworldavatar.com/ontology/OntoFHRS/startperiod/1> a ies4:ParticularPeriod;
    ies4:iso8601PeriodRepresentation "2015-01-01"^^xsd:date.

<http://www.theworldavatar.com/ontology/OntoFHRS/endperiod/1> a ies4:ParticularPeriod;
    ies4:iso8601PeriodRepresentation "2015-12-31"^^xsd:date.
```

### Opening hours

Each business establishment is allowed to have multiple schedules, types of schedules allowed:

- `<https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/RegularSchedule>`
  - Schedules that repeat (e.g. standard weekday opening hours)
- `<https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/AdHocSchedule>`
  - Special schedules that overwrite regular schedules

Example of a regular schedule for Monday and Tuesday, opening from 09:00 to 20:00. When regular schedules exist, weekdays without a schedule are closed. With no regular schedules and no ad hoc schedule for the date, the business is assumed to be open at all times. Note that fibo-fnd-fd:hasRecurrenceInterval is mandatory, whereas cmns-dt:hasTimePeriod, cmns-dt:hasStartDate/cmns-dt:hasEndDate are optional.

```turtle
PREFIX fh: <http://www.theworldavatar.com/ontology/OntoFHRS/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX cmns-dt: <https://www.omg.org/spec/Commons/DatesAndTimes/>
PREFIX fibo-fnd-fd: <https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/>
PREFIX ex: <http://example.org/>

ex:foodretail a fh:BusinessEstablishment;
    fibo-fnd-fd:hasSchedule ex:schedule.
ex:schedule a fibo-fnd-fd:RegularSchedule;
    fibo-fnd-fd:hasRecurrenceInterval fibo-fnd-fd:Monday; # mandatory property
    fibo-fnd-fd:hasRecurrenceInterval fibo-fnd-fd:Tuesday;
    cmns-dt:hasTimePeriod ex:period; # optional property
    cmns-dt:hasEndDate ex:endDate; # optional property
    cmns-dt:hasStartDate ex:startDate. # optional property
ex:startDate a cmns-dt:Date;
    cmns-dt:hasDateValue "2025-01-01"^^xsd:date.
ex:endDate a cmns-dt:Date;
    cmns-dt:hasDateValue "2025-12-31"^^xsd:date.
ex:period a cmns-dt:ExplicitTimePeriod;
    cmns-dt:hasStartTime ex:startTime;
    cmns-dt:hasEndTime ex:endTime.
ex:startTime a cmns-dt:TimeOfDay;
    cmns-dt:hasTimeValue "09:00:00"^^xsd:time.
ex:endTime a cmns-dt:TimeOfDay;
    cmns-dt:hasTimeValue "20:00:00"^^xsd:time.
```

Allowed recurrence intervals:

- fibo-fnd-fd:Monday
- fibo-fnd-fd:Tuesday
- fibo-fnd-fd:Wednesday
- fibo-fnd-fd:Thursday
- fibo-fnd-fd:Friday
- fibo-fnd-fd:Saturday
- fibo-fnd-fd:Sunday

Example of an ad hoc schedule note that cmns-col:hasMember is mandatory, whereas cmns-dt:hasStartDate, cmns-dt:hasEndDate and cmns-dt:hasTimePeriod are optional:

```turtle
PREFIX fh: <http://www.theworldavatar.com/ontology/OntoFHRS/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX cmns-dt: <https://www.omg.org/spec/Commons/DatesAndTimes/>
PREFIX fibo-fnd-fd: <https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/>
PREFIX cmns-col:    <https://www.omg.org/spec/Commons/Collections/>
PREFIX ex: <http://example.org/>

ex:foodretail a fh:BusinessEstablishment;
    fibo-fnd-fd:hasSchedule ex:public_holiday_schedule.
ex:public_holiday_schedule a fibo-fnd-fd:AdHocSchedule;
    cmns-col:hasMember ex:member1; # mandatory property
    cmns-col:hasMember ex:member2;
    cmns-dt:hasStartDate ex:startdate; # optional property
    cmns-dt:hasEndDate ex:enddate; # optional property
    cmns-dt:hasTimePeriod ex:period. # optional property

ex:member1 a fibo-fnd-fd:AdHocScheduleEntry;
    cmns-dt:hasDate ex:member1_date.

ex:member2 a fibo-fnd-fd:AdHocScheduleEntry;
    cmns-dt:hasDate ex:member2_date.

ex:member1_date a cmns-dt:Date; cmns-dt:hasDateValue "2025-12-25"^^xsd:date.
ex:member2_date a cmns-dt:Date; cmns-dt:hasDateValue "2025-01-01"^^xsd:date.

ex:startdate a cmns-dt:Date; cmns-dt:hasDateValue "2025-01-01"^^xsd:date.
ex:enddate a cmns-dt:Date; cmns-dt:hasDateValue "2025-12-31"^^xsd:date.

ex:period a cmns-dt:ExplicitTimePeriod;
    cmns-dt:hasStartTime ex:startTime;
    cmns-dt:hasEndTime ex:endTime.
ex:startTime a cmns-dt:TimeOfDay;
    cmns-dt:hasTimeValue "10:00:00"^^xsd:time.
ex:endTime a cmns-dt:TimeOfDay;
    cmns-dt:hasTimeValue "16:00:00"^^xsd:time.
```

Schedule validity dates are applied only when both start and end dates are provided; a single date bound is ignored. A schedule with no complete time period is treated as closed. This allows an ad hoc schedule without a time period to represent a closure.

### Opening hours crossing midnight

Opening hours such as Monday 1000 - 0100 should be instantiated as Monday 1000 - 2359 & Tuesday 0000 - 0100, where the end time should always be larger than the start time.

### Trip containment

The filtering of opening hours differ slightly depending on which calculation type is specified.

`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryTimeFilterCount>`

- This approach requires full trip containment for an exposure to be valid.
- Filter is done in 1 step.
- Entire trip needs to be within the opening hours of a feature for the exposure to be valid, for example, consider a business with opening hours of 1000-2200. A trip spanning between 0900-1100 will not be exposed to this business, whereas a trip spanning between 1100-1200 is exposed to this business.

`<https://www.theworldavatar.com/kg/ontoexposure/TrajectoryTimeFilterCountDetailed>`

- This approach is more detailed and the filtering steps are detailed below:
  - During the trip filter stage, partial overlap is allowed, e.g. a trip spanning 0900-1100 will pass the filter for the opening hours of 1000-2200
  - If the first filter passes, the closest recorded GPS point within the trip from the business is obtained, then the timestamp of this recorded point is compared with the opening hours of the business. Trip is considered to be exposed if the timestamp of the closest point is within the opening hours.


### Current implementation limits

- Full containment requires each daily trip segment to fit within a single opening period; adjacent periods are not merged.
- If the closest recorded coordinate occurs multiple times, any matching timestamp within opening hours can qualify.
