PREFIX_EXPOSURE = 'https://www.theworldavatar.com/kg/ontoexposure/'

# default columns from gdal
VECTOR_GEOMETRY_COLUMN = 'wkb_geometry'
RASTER_GEOMETRY_COLUMN = 'rast'

# predicates
HAS_DISTANCE = PREFIX_EXPOSURE + 'hasDistance'
HAS_UPPERBOUND = PREFIX_EXPOSURE + 'hasUpperbound'
HAS_LOWERBOUND = PREFIX_EXPOSURE + 'hasLowerBound'
DCTERM_TITLE = 'http://purl.org/dc/terms/title'

# time series related
TIMESERIES_NAMESPACE = 'https://www.theworldavatar.com/kg/ontotimeseries/'
HAS_TIME_SERIES = TIMESERIES_NAMESPACE + 'hasTimeSeries'
TIMESERIES_TYPE = TIMESERIES_NAMESPACE + 'TimeSeries'
HAS_TIME_CLASS = TIMESERIES_NAMESPACE + 'hasTimeClass'

# types
DCAT_DATASET = 'http://www.w3.org/ns/dcat#Dataset'

TRAJECTORY_COUNT = PREFIX_EXPOSURE + 'TrajectoryCount'
TRIP = PREFIX_EXPOSURE + 'Trip'
CALCULATION_TYPES = [TRAJECTORY_COUNT]
