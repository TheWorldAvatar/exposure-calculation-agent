PREFIX_EXPOSURE = 'https://www.theworldavatar.com/kg/ontoexposure/'
PREFIX_DCAT = 'http://www.w3.org/ns/dcat#'
PREFIX_DERIVATION = 'https://www.theworldavatar.com/kg/ontoderivation/'

# default columns from gdal
VECTOR_GEOMETRY_COLUMN = 'wkb_geometry'
RASTER_GEOMETRY_COLUMN = 'rast'

# predicates
HAS_DISTANCE = PREFIX_EXPOSURE + 'hasDistance'
HAS_UPPERBOUND = PREFIX_EXPOSURE + 'hasUpperbound'
HAS_LOWERBOUND = PREFIX_EXPOSURE + 'hasLowerbound'
HAS_VALUE = PREFIX_EXPOSURE + 'hasValue'
DCTERM_TITLE = 'http://purl.org/dc/terms/title'
DATASET_PREDICATE = PREFIX_DCAT + 'dataset'
SERVES_DATASET = PREFIX_DCAT + 'servesDataset'
ENDPOINT_URL = PREFIX_DCAT + 'endpointURL'
IS_DERIVED_FROM = PREFIX_DERIVATION + 'isDerivedFrom'
BELONGS_TO = PREFIX_DERIVATION + 'belongsTo'
HAS_CALCULATION_METHOD = PREFIX_EXPOSURE + 'hasCalculationMethod'
HAS_GEOMETRY_COLUMN = PREFIX_EXPOSURE + 'hasGeometryColumn'
HAS_VALUE_COLUMN = PREFIX_EXPOSURE + 'hasValueColumn'

# time series related
TIMESERIES_NAMESPACE = 'https://www.theworldavatar.com/kg/ontotimeseries/'
HAS_TIME_SERIES = TIMESERIES_NAMESPACE + 'hasTimeSeries'
TIMESERIES_TYPE = TIMESERIES_NAMESPACE + 'TimeSeries'
HAS_TIME_CLASS = TIMESERIES_NAMESPACE + 'hasTimeClass'

# types
DCAT_DATASET = PREFIX_DCAT + 'Dataset'
TRIP = PREFIX_EXPOSURE + 'Trip'
EXPOSURE_RESULT = PREFIX_EXPOSURE + 'ExposureResult'
DERIVATION = PREFIX_DERIVATION + 'Derivation'
POSTGIS_SERVICE = 'https://theworldavatar.io/kg/service#PostGIS'

TRAJECTORY_COUNT = PREFIX_EXPOSURE + 'TrajectoryCount'
TRAJECTORY_AREA = PREFIX_EXPOSURE + 'TrajectoryArea'
SIMPLE_COUNT = PREFIX_EXPOSURE + 'Count'
SIMPLE_AREA = PREFIX_EXPOSURE + 'Area'
AREA_WEIGHTED_SUM = PREFIX_EXPOSURE + 'AreaWeightedSum'
CALCULATION_TYPES = [TRAJECTORY_COUNT, SIMPLE_COUNT,
                     SIMPLE_AREA, TRAJECTORY_AREA, AREA_WEIGHTED_SUM]
TRAJECTORY_TYPES = [TRAJECTORY_COUNT, TRAJECTORY_AREA]

BIND_MOUNT_PATH = '/app/queries/'  # needs to match with stack manager config

# units
METRE_SQUARED = 'm²'
