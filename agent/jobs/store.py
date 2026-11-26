"""Small in-memory job store, shared with child processes via a local manager."""
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from multiprocessing.managers import BaseManager
import os
import threading
from uuid import uuid4

def now():
    return datetime.now(timezone.utc)


class JobStore:
    def __init__(self):
        self._jobs = {}
        self._pending = deque()
        self._running_owners = set()
        self._lock = threading.RLock()

    def enqueue(self, payload, owner_id=None):
        with self._lock:
            job_id = str(uuid4())
            self._jobs[job_id] = {
                'id': job_id, 'owner_id': owner_id, 'payload': deepcopy(payload),
                'status': 'queued', 'stage': 'queued', 'result': None, 'error': None,
                'worker_id': None, 'created_at': now(), 'started_at': None,
                'finished_at': None, 'heartbeat_at': None,
            }
            self._pending.append(job_id)
            return job_id

    def get(self, job_id):
        with self._lock:
            job = self._jobs.get(str(job_id))
            if job is None:
                return None
            return deepcopy({k: v for k, v in job.items() if k not in ('payload', 'worker_id')})

    def active_for_owner(self, owner_id):
        with self._lock:
            return deepcopy([
                {k: v for k, v in job.items() if k not in ('payload', 'worker_id')}
                for job in self._jobs.values()
                if owner_id is not None and job['owner_id'] == owner_id
                and job['status'] in ('queued', 'running')
            ])

    def claim(self, worker_id):
        with self._lock:
            for index, job_id in enumerate(self._pending):
                job = self._jobs[job_id]
                owner_id = job['owner_id']
                if owner_id in self._running_owners:
                    continue
                del self._pending[index]
                job.update(status='running', stage='starting', worker_id=str(worker_id),
                           started_at=now(), heartbeat_at=now())
                if owner_id is not None:
                    self._running_owners.add(owner_id)
                return deepcopy({k: job[k] for k in ('id', 'owner_id', 'payload')})
            return None

    def heartbeat(self, job_id, worker_id):
        self._change(job_id, worker_id, heartbeat_at=now())

    def progress(self, job_id, worker_id, stage):
        self._change(job_id, worker_id, stage=stage, heartbeat_at=now())

    def finish(self, job_id, worker_id, result=None, error=None):
        changes = dict(status='failed' if error else 'succeeded', result=result,
                       error=error, finished_at=now())
        if not error:
            changes['stage'] = 'completed'
        self._change(job_id, worker_id, **changes)

    def _change(self, job_id, worker_id, **changes):
        with self._lock:
            job = self._jobs.get(str(job_id))
            if job and job['status'] == 'running' and job['worker_id'] == str(worker_id):
                job.update(changes)
                if job['status'] in ('succeeded', 'failed'):
                    self._running_owners.discard(job['owner_id'])


_store = JobStore()


def shared_store():
    return _store


class ExposureQueueManager(BaseManager):
    pass


ExposureQueueManager.register('get_store', callable=shared_store)


def start_queue_server():
    """Owned by the launcher, so jobs survive an API worker restart only."""
    secret = os.urandom(32)
    manager = ExposureQueueManager(address=('127.0.0.1', 0), authkey=secret)
    server = manager.get_server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return {'EXPOSURE_QUEUE_PORT': str(server.address[1]), 'EXPOSURE_QUEUE_AUTHKEY': secret.hex()}


class QueueClient:
    def __init__(self):
        self._proxy = None
        self._manager = None
        self._lock = threading.Lock()

    def _store(self):
        with self._lock:
            if self._proxy is None:
                if 'EXPOSURE_QUEUE_PORT' not in os.environ or 'EXPOSURE_QUEUE_AUTHKEY' not in os.environ:
                    raise RuntimeError('Start the API and calculation worker with python -m agent.run')
                manager = ExposureQueueManager(
                    address=('127.0.0.1', int(os.environ['EXPOSURE_QUEUE_PORT'])),
                    authkey=bytes.fromhex(os.environ['EXPOSURE_QUEUE_AUTHKEY']))
                manager.connect()
                self._proxy = manager.get_store()
                self._manager = manager
            return self._proxy

    def enqueue(self, payload, owner_id=None):
        return self._store().enqueue(payload, owner_id=owner_id)

    def get(self, job_id):
        return self._store().get(job_id)

    def active_for_owner(self, owner_id):
        return self._store().active_for_owner(owner_id)

    def claim(self, worker_id):
        return self._store().claim(worker_id)

    def heartbeat(self, job_id, worker_id):
        self._store().heartbeat(job_id, worker_id)

    def progress(self, job_id, worker_id, stage):
        self._store().progress(job_id, worker_id, stage)

    def finish(self, job_id, worker_id, result=None, error=None):
        self._store().finish(job_id, worker_id, result=result, error=error)


jobs = QueueClient()
