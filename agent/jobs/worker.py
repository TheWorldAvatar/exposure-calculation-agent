"""Run separately from Gunicorn: python -m agent.jobs.worker."""
from twa import agentlogging
import threading
import time
from uuid import uuid4

from agent.jobs.store import jobs
from agent.jobs.timeline import execute_job as execute_timeline_job

logger = agentlogging.get_logger('dev')


def execute_job(job, store, worker_id):
    kind = job['payload'].get('kind')
    if kind in ('trigger_single', 'trigger_bulk'):
        from agent.interactor.trigger_calculation import run_calculation, run_bulk_calculation
        store.progress(job['id'], worker_id, 'calculating_exposure')
        execute = run_bulk_calculation if kind == 'trigger_bulk' else run_calculation
        return {'message': execute(job['payload']['inputs'])}
    return execute_timeline_job(job, store, worker_id)


def run_job(job, worker_id, store=jobs, execute=execute_job):
    started = time.perf_counter()
    stopped = threading.Event()

    def keep_alive():
        while not stopped.wait(30):
            try:
                store.heartbeat(job['id'], worker_id)
            except Exception:
                logger.exception('Heartbeat failed for job %s', job['id'])

    heartbeat = threading.Thread(target=keep_alive, daemon=True)
    heartbeat.start()
    try:
        logger.info('Starting exposure job %s', job['id'])
        result = execute(job, store, worker_id)
        store.finish(job['id'], worker_id, result=result)
        logger.info('Exposure job %s completed successfully in %.2f seconds: %s',
                    job['id'], time.perf_counter() - started, result)
    except Exception:
        logger.exception('Exposure job %s failed after %.2f seconds',
                         job['id'], time.perf_counter() - started)
        # Detailed database/endpoint errors stay in logs, not API responses.
        store.finish(job['id'], worker_id, error='Exposure calculation failed; see worker logs for job ' + job['id'])
    finally:
        stopped.set()
        heartbeat.join(timeout=2)


def main():
    worker_id = str(uuid4())
    logger.info('Background exposure worker %s started', worker_id)
    while True:
        try:
            job = jobs.claim(worker_id)
            if job is None:
                time.sleep(2)
            else:
                run_job(job, worker_id)
        except Exception:
            logger.exception('Job queue operation failed')
            time.sleep(5)


if __name__ == '__main__':
    main()
