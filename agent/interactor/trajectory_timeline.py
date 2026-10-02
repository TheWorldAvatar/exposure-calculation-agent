"""Authenticated exposure calculations for the current user's trajectories."""
from datetime import datetime
from functools import lru_cache
import json
import math
import re
from urllib.parse import quote

from flask import Blueprint, request
import jwt
from jwt import PyJWKClient

from agent.utils.env_configs import KEYCLOAK_SERVER, KEYCLOAK_REALM
from agent.utils import constants

trajectory_timeline_bp = Blueprint('trajectory_timeline', __name__)


class AuthenticationError(Exception):
    pass


@lru_cache(maxsize=1)
def get_keycloak_jwks_client():
    issuer = f"{KEYCLOAK_SERVER.rstrip('/')}/realms/{KEYCLOAK_REALM}"
    return PyJWKClient(f'{issuer}/protocol/openid-connect/certs')


def get_authenticated_user_id(authorization):
    if not authorization or not authorization.lower().startswith('bearer '):
        raise AuthenticationError('Bearer token is missing')
    token = authorization[7:].strip()
    if not token:
        raise AuthenticationError('Bearer token is missing')
    if not KEYCLOAK_SERVER or not KEYCLOAK_REALM:
        raise AuthenticationError('Keycloak authentication is not configured')
    issuer = f"{KEYCLOAK_SERVER.rstrip('/')}/realms/{KEYCLOAK_REALM}"
    try:
        key = get_keycloak_jwks_client().get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token, key.key, algorithms=['RS256'], issuer=issuer,
            options={'require': ['exp', 'sub'], 'verify_aud': False})
    except jwt.PyJWTError as ex:
        raise AuthenticationError('Invalid or expired bearer token') from ex
    user_id = claims.get('sub')
    if not isinstance(user_id, str) or not user_id.strip():
        raise AuthenticationError('Bearer token subject is missing')
    return user_id


def get_owned_point_iris(user_id):
    from agent.utils.kg_client import kg_client
    person = 'https://w3id.org/MON/person.owl#person_' + quote(user_id, safe='-._~')
    query = f"""
        SELECT DISTINCT ?point WHERE {{
            <{person}> <https://www.theworldavatar.com/kg/sensorloggerapp/hasA>/
                <https://saref.etsi.org/core/consistsOf>/
                <https://www.theworldavatar.com/kg/ontodevice/hasGeoLocation> ?point.
        }}
        ORDER BY ?point
    """
    rows = json.loads(kg_client.remote_store_client.executeQuery(query).toString())
    return [row['point'] for row in rows]


def parse_settings(args):
    if any(key in args for key in ('subject', 'subject_query_file', 'user_id', 'userId')):
        raise ValueError('Trajectory ownership is determined by the bearer token')
    rdf_type = args.get('rdf_type')
    if rdf_type not in constants.TRAJECTORY_TYPES:
        raise ValueError('rdf_type must be a trajectory calculation type')
    dataset_iri = args.get('dataset_iri', '').strip()
    if (not re.match(r'^[A-Za-z][A-Za-z0-9+.-]*:.+$', dataset_iri)
            or any(c.isspace() or ord(c) < 32 or c in '<>"{}|^`\\' for c in dataset_iri)):
        raise ValueError('Provide a valid absolute dataset_iri')
    try:
        distance = float(args.get('distance', ''))
    except (ValueError, TypeError) as ex:
        raise ValueError('distance must be a non-negative finite number') from ex
    if not math.isfinite(distance) or distance < 0:
        raise ValueError('distance must be a non-negative finite number')
    bounds = {}
    parsed = {}
    for name in ('lowerbound', 'upperbound'):
        value = args.get(name)
        if value is not None:
            try:
                parsed[name] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                if parsed[name].utcoffset() is None:
                    raise ValueError()
            except (ValueError, TypeError) as ex:
                raise ValueError(f'{name} must be an ISO datetime with a timezone') from ex
            value = parsed[name].isoformat()
        bounds[name] = value
    if len(parsed) == 2 and parsed['lowerbound'] > parsed['upperbound']:
        raise ValueError('lowerbound must not exceed upperbound')
    return rdf_type, dataset_iri, distance, bounds


@trajectory_timeline_bp.route('/calculate_exposure_for_timeline', methods=['POST'])
def calculate_exposure_for_timeline():
    """Calculate exposure for the authenticated user's trajectory points.

    Required header:
        Authorization: Bearer <access-token> (a valid Keycloak user token).

    Required query parameters:
        rdf_type: Calculation type IRI from constants.TRAJECTORY_TYPES.
        dataset_iri: Absolute exposure dataset IRI, passed directly to calculation.
        distance: Buffer distance in metres; must be finite and non-negative.

    Optional query parameters:
        lowerbound, upperbound: ISO datetimes with timezones (inclusive bounds).
            If both are supplied, lowerbound must not exceed upperbound.

    Trajectory point IRIs are resolved from the token's user identity;
    caller-supplied subject or user identity parameters are not accepted.
    """
    try:
        user_id = get_authenticated_user_id(request.headers.get('Authorization'))
    except AuthenticationError as ex:
        return str(ex), 401, {'WWW-Authenticate': 'Bearer'}
    try:
        rdf_type, dataset_iri, distance, bounds = parse_settings(request.args)
    except ValueError as ex:
        return str(ex), 400

    point_iris = get_owned_point_iris(user_id)
    if not point_iris:
        return 'No trajectory points found for authenticated user', 404

    from agent.interactor.initialise_calculation import initialise_calculation
    from agent.objects.calculation_metadata import CalculationMetadata
    from agent.calculation.api import do_calculation
    calculation = initialise_calculation(CalculationMetadata(
        rdf_type=rdf_type, distance=distance, **bounds))
    try:
        return do_calculation(subject=point_iris, calculation=calculation, exposure=dataset_iri,
                              timeline=True)
    except ValueError as ex:
        return str(ex), 400
