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

Exposure dataset needs to have IRI column specified:

```ttl
<http://exposure> a <https://www.theworldavatar.com/kg/ontoexposure/ExposureDataset>;
    <https://www.theworldavatar.com/kg/ontoexposure/hasIriColumn> "iri";
    <https://www.theworldavatar.com/kg/ontoexposure/hasGeometryColumn> "wkb_geometry".
```

## Time zone

Time zone information is required to convert local opening hours into timestamps. Time zone data is uploaded as part of the [HD4-stack](https://github.com/TheWorldAvatar/hd4-stack/).

Each time zone is represented by a polygon and a time zone ID, e.g.

```ttl
PREFIX exposure:   <https://www.theworldavatar.com/kg/ontoexposure/>

exposure:timezone/1 a exposure:TimeZone;
    geo:asWKT "POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))"^^geo:wktLiteral;
    exposure:tzid "Europe/London".
```

## Workflow

1) Geospatial filtering
   1) Construct a line for each trip
   2) Collect IRIs of intersected features for each trip
2) Time filtering
   1) Query business start/end and opening hours of each intersected feature
   2) Do time filtering, refer to [time filtering hierarcy](#time-filtering-hierarchy) section for more details

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

The example below shows the start and end times for the instance `<http://www.theworldavatar.com/ontology/OntoFHRS/Business/1>`.

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

Each business establishment is allowed to have multiple schedules, types of schedules allowed:

- `<https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/RegularSchedule>`
  - Schedules that repeat (e.g. standard weekday opening hours)
- `<https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/AdHocSchedule>`
  - Special schedules that overwrite regular schedules

Example of a regular schedule that repeats every Monday - Tuesday, opening from 0900 to 2000, days without schedules are assumed to be closed. Note that fibo-fnd-fd:hasRecurrenceInterval is mandatory, whereas cmns-dt:hasTimePeriod, cmns-dt:hasStartDate/cmns-dt:hasEndDate are optional.

```text
PREFIX fh: <http://www.theworldavatar.com/ontology/OntoFHRS/>
PREFIX cmns-dt: <https://www.omg.org/spec/Commons/DatesAndTimes/>
PREFIX fibo-fnd-fd: <https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/>
PREFIX ex: <http://example.org>

ex:foodretail a fh:BusinessEstablishment;
    fibo-fnd-fd:hasSchedule ex:schedule.
ex:schedule a fibo-fnd-fd:RegularSchedule;
    fibo-fnd-fd:hasRecurrenceInterval fibo-fnd-fd:Monday; # mandatory property
    fibo-fnd-fd:hasRecurrenceInterval fibo-fnd-fd:Tuesday;
    cmns-dt:hasTimePeriod ex:period; # optional property
    cmns-dt:hasEndDate ex:endDate; # optional property
    cmns-dt:hasStartDate ex:startDate. # optional property
ex:startDate a cmns-dt:Date;
    cmns-dt:hasDateValue “01-01-2025”^^xsd:date.
ex:endDate a cmns-dt:Date;
    cmns-dt:hasDateValue “31-12-2025”^^xsd:date.
ex:period a cmns:ExplicitTimePeriod;
    cmns-dt:hasStartTime ex:startTime;
    cmns-dt:hasEndTime ex:endTime.
ex:startTime a cmns-dt:TimeOfDay;
    cmns-dt:hasTimeValue “09:00”^^xsd:time.
ex:endTime a cmns-dt:TimeOfDay;
    cmns-dt:hasTimeValue “20:00”^^xsd:time.
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

```text
PREFIX fh: <http://www.theworldavatar.com/ontology/OntoFHRS/>
PREFIX cmns-dt: <https://www.omg.org/spec/Commons/DatesAndTimes/>
PREFIX fibo-fnd-fd: <https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/>
PREFIX cmns-col:    <https://www.omg.org/spec/Commons/Collections/>
PREFIX ex: <http://example.org>

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

ex:member1_date a cmns-dt:Date; cmns-dt:hasDateValue "25-12-2025"^^xsd:date.
ex:member2_date a cmns-dt:Date; cmns-dt:hasDateValue "01-01-2025"^^xsd:date.

ex:startdate a cmns-dt:Date; cmns-dt:hasDateValue "01-01-2025"^^xsd:date.
ex:enddate a cmns-dt:Date; cmns-dt:hasDateValue "31-12-2025"^^xsd:date.

ex:period a cmns:ExplicitTimePeriod;
    cmns-dt:hasStartTime ex:startTime;
    cmns-dt:hasEndTime ex:endTime.
ex:startTime a cmns-dt:TimeOfDay"
    cmns-dt:hasTimeValue "10:00"^^xsd:time.
ex:endTime a cmns-dt:TimeOfDay;
    cmns-dt:hasTimeValue "16:00"^^xsd:time.
```

### Opening hours crossing midnight

Note that the for opening hours like Monday 1000 - 0100 should be instantiated as Monday 1000 - 2359 & Tuesday 0000 - 0100, where the end time should always be larger than the start time.

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
