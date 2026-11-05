"""Bounded recovery for interrupted time-series SELECT responses."""
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

# Share the application's configured logger so INFO submissions appear in the
# same output as KgClient, rather than depending on the root logger's level.
logger = logging.getLogger('dev')


def is_range_failure(error):
    """Only recover failures consistent with an interrupted/expired response."""
    message = str(error).lower()
    return any(marker in message for marker in (
        'eofexception', 'end of input', 'timed out', 'timeout',
        'connection reset', 'premature end', 'incomplete read',
        'unterminated string',
    ))


def fetch_time_series_rows(execute, prefix, pattern, conditions, axis=None,
                           lower=None, upper=None, max_depth=12):
    """Try once normally, then bisect failed ranges, retaining inclusive endpoints.

    ``execute`` must return fully parsed rows or raise; partial batches are never
    committed. Bounds are SPARQL expressions already validated by the caller.
    """
    def query(extra=(), projection='?timestamp ?time_number ?val ?measure', order=True):
        filters = list(conditions) + list(extra)
        where = pattern
        if filters:
            where += '\nFILTER (' + ' && '.join(filters) + ')'
        return (prefix + '\nSELECT ' + projection + '\nWHERE {\n' + where
                + '\n}\n' + ('ORDER BY ?timestamp ?time_number' if order else ''))

    try:
        logger.info('Submitting original time-series query (filters: %s)',
                    ' && '.join(conditions) or 'none')
        return execute(query())
    except Exception as error:
        if not is_range_failure(error):
            raise
        logger.warning('Time-series query interrupted; recovering with time ranges: %s', error)

    # Only discover bounds after the ordinary request has failed. Use the same
    # pattern and filters so extrema describe precisely the requested rows.
    if lower is None or upper is None:
        logger.info('Submitting time-series MIN/MAX query to discover missing bounds')
        extrema = execute(query(projection=(
            '(MIN(?timestamp) AS ?minTimestamp) (MAX(?timestamp) AS ?maxTimestamp) '
            '(MIN(?time_number) AS ?minNumber) (MAX(?time_number) AS ?maxNumber)'),
            order=False))
        bounds = extrema[0] if extrema else {}
        if axis is None:
            axis = '?time_number' if 'minNumber' in bounds else '?timestamp'
        suffix = 'Number' if axis == '?time_number' else 'Timestamp'
        if 'min' + suffix not in bounds or 'max' + suffix not in bounds:
            raise RuntimeError('Cannot split failed query: no usable time bounds')

        def expression(value):
            if axis == '?time_number':
                return str(Decimal(value))
            return json.dumps(value) + '^^xsd:dateTime'

        lower = lower if lower is not None else expression(bounds['min' + suffix])
        upper = upper if upper is not None else expression(bounds['max' + suffix])

    def number(value):
        return Decimal(value)

    def date(value):
        return datetime.fromisoformat(json.loads(value.split('^^')[0]))

    parse = number if axis == '?time_number' else date
    start, end = parse(lower), parse(upper)

    def literal(value):
        if axis == '?time_number':
            return str(value)
        return json.dumps(value.isoformat()) + '^^xsd:dateTime'

    def split(lo, hi, inclusive, depth):
        midpoint = (lo + hi) / 2 if axis == '?time_number' else lo + (hi - lo) / 2
        if depth >= max_depth or not lo < midpoint < hi:
            raise RuntimeError(
                'Time-range recovery exhausted: interval cannot be split further '
                '(too many rows at one time or persistent endpoint failure)')
        logger.info('Splitting time-series range %s to %s at %s (depth %s)',
                    lo, hi, midpoint, depth)
        return fetch(lo, midpoint, False, depth + 1) + fetch(midpoint, hi, inclusive, depth + 1)

    def fetch(lo, hi, inclusive, depth):
        try:
            logger.info('Submitting time-series range query: %s >= %s AND %s %s %s (depth %s)',
                        axis, literal(lo), axis, '<=' if inclusive else '<', literal(hi), depth)
            return execute(query((f'{axis} >= {literal(lo)}',
                                  f'{axis} {"<=" if inclusive else "<"} {literal(hi)}')))
        except Exception as error:
            if not is_range_failure(error):
                raise
            return split(lo, hi, inclusive, depth)

    rows = split(start, end, True, 0)
    # Every trajectory consistently uses one time representation, so the
    # selected axis covers all its observations.

    def sort_key(row):
        timestamp = row.get('timestamp')
        instant = datetime.fromisoformat(timestamp) if timestamp is not None else None
        if instant is not None:
            instant = instant.replace(tzinfo=timezone.utc) if instant.tzinfo is None else instant
        return (timestamp is not None, instant,
                'time_number' in row, Decimal(row.get('time_number', '0')))

    return sorted(rows, key=sort_key)
