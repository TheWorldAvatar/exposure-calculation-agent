"""Execute one validated timeline exposure request."""
from twa import agentlogging

logger = agentlogging.get_logger('dev')

def execute_job(job, store, worker_id):
    from agent.interactor.trajectory_timeline import get_owned_point_iris
    from agent.interactor.initialise_calculation import initialise_calculation
    from agent.objects.calculation_metadata import CalculationMetadata
    from agent.calculation.api import do_calculation

    payload = job['payload']

    def progress(stage):
        store.progress(job['id'], worker_id, stage)
        logger.info('Exposure job %s stage: %s', job['id'], stage)
    progress('resolving_owned_trajectories')
    point_iris = get_owned_point_iris(job['owner_id'])
    if not point_iris:
        raise ValueError('No trajectory points found for this user')
    progress('initialising_calculation')
    calculation = initialise_calculation(CalculationMetadata(
        rdf_type=payload['rdf_type'], distance=payload['distance'],
        dataset_filter=payload['dataset_filter'], **payload['bounds']))
    progress('calculating_exposure')
    message, status = do_calculation(
        subject=point_iris, calculation=calculation,
        exposure=payload['dataset_iri'], timeline=True)
    if not 200 <= status < 300:
        raise ValueError(f'Calculation returned HTTP {status}: {message}')
    return {'message': message, 'calculation_iri': calculation}


