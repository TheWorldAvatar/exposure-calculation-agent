# Changelog

## 2.6.0

- Improved query performance by using dedicated subject and exposure dataset predicates in Ontop mappings.

## 2.5.0

- Added trajectory raster area and average calculations.
- Fixed trajectory raster clipping and added support for the raster's native CRS.
- Added dataset filters and optional trips to Timeline calculations.
- Updated Stack Clients and documentation.
- Fixed opening-hours overlap checks.

## 2.4.0

- Added authenticated Timeline calculations across multiple devices.
- Separated consecutive stays by recording session.
- Fixed a container startup issue.

## 2.3.1

- Fixed trajectory time bounds and calculation matching.

## 2.3.0

- Added closest-distance, total-length, areal, average, and weighted-average calculations.
- Added result sets, correlation, clustering, and GeoServer layers.
- Improved null handling and CSV export.
- Prevented double-counting of overlapping polygon areas.
- Added raster average, reimplemented raster sum, and removed raster count.
- Added development tools to the debug image.

## 2.2.0

- Added raster sum calculations.
- Added summary statistics to exposure results.
- Fixed calculation lookup for requests without dataset filters.

## 2.1.0

- Added trajectory filtering by business dates and opening hours.
- Supported regular and ad hoc schedules, time zones, and trips spanning multiple days.
- Added raster clipping, raster count, and raster area calculations.
- Added dataset filters to count and area calculations.
- Added bulk calculations and SQL result queries.
- Updated CSV export and fixed trajectory and geometry lookup issues.

## 2.0.2

- Improved logging, Ontop mapping uploads, and cache refresh.
- Improved trajectory CSV export.
- Updated dependencies and improved Podman compatibility.

## 2.0.1

- Made the exposure geometry column optional to specify.

## 2.0.0

- Switched trajectory time-series queries to SPARQL through Ontop and outgoing federation.
- Added trajectory area-weighted sum calculations.
- Combined multiple exposure datasets into a single CSV export.
- Improved namespace initialization and dataset metadata handling.

## 1.2.0

- Added area-weighted sum calculations and configurable dataset columns.
- Replaced the earlier raster calculations with area-weighted processing.
- Improved performance and fixed CSV export for subjects with different results.

## 1.1.0

Added instantiation of units

## 1.0.0

First version
