# Authenticated trajectory exposure calculations

This is designed to be called by the [Timeline app](<https://github.com/TheWorldAvatar/TimelineApp>).

`POST /calculate_exposure_for_timeline` calculates exposure across the authenticated
user's devices. A user can have multiple devices if they reinstall the app.

Configure these environment variables in the deployed agent:

- `KEYCLOAK_SERVER`: Keycloak base URL, matching the token issuer's base URL.
- `KEYCLOAK_REALM`: realm name.

Example request:

```http
POST /calculate_exposure_for_timeline?rdf_type=https://www.theworldavatar.com/kg/ontoexposure/TrajectoryCount&dataset_iri=https://example.com/dataset/parks&distance=10&lowerbound=2026-09-01T00:00:00Z&upperbound=2026-09-02T00:00:00Z
Authorization: Bearer <access-token>
```

Required query parameters are `rdf_type`, `dataset_iri`, and `distance` in metres.
Optional `lowerbound` and `upperbound` must be ISO datetimes with timezones; filtering
is inclusive. A bounded request calculates over the selected observations, which
can be a partial trip. Numeric epoch bounds are not accepted by this endpoint.

Optional `dataset_filter` accepts a JSON object, for example `{"year":2016}`, this will add something like `WHERE year=2016` to the SQL queries for calculations.

Trips are optional. If all devices contributing observations lack trip measures, the entire selected trajectory, merged chronologically across devices, is treated as one trip. Devices with observations must either all have trip measures or all omit them.

When using trip measures, run the [trip-agent](<https://github.com/TheWorldAvatar/trip-agent>) joint trip detection first, especially if the selected time range includes data from multiple devices. The trip agent labels trips in increasing indices (1, 2, 3, etc.) over the combined devices. If the trip processing is done separately for each device, the calculation can fail because the trip index restarts at 1 for subsequent devices. The endpoint checks for inconsistent trip labels, but cannot verify that all devices were processed together.
