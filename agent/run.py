"""Start the HTTP server and four independent calculation workers."""
import os
import signal
import subprocess
import sys
import time
from agent.jobs.store import start_queue_server


def api_command(debug=False):
    if debug:
        return [sys.executable, '-m', 'debugpy', '--listen', '0.0.0.0:5678',
                '--wait-for-client', '-m', 'flask', 'run', '-h', '0.0.0.0', '-p', '5000']
    return ['gunicorn', '--bind', '0.0.0.0:5000', '--timeout', '600',
            '-w', '4', '--threads', '8', 'agent.app:app']


def signal_process_group(child, signum):
    """Include Java gateway children when stopping a service process."""
    try:
        os.killpg(child.pid, signum)
    except ProcessLookupError:
        pass


def stop_children(children):
    for child in children:
        signal_process_group(child, signal.SIGTERM)
    deadline = time.monotonic() + 8
    for child in children:
        try:
            child.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            signal_process_group(child, signal.SIGKILL)
            child.wait()


def main():
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    environment = os.environ.copy()
    environment.update(start_queue_server())
    children = []
    result = 0
    try:
        for _ in range(4):
            children.append(subprocess.Popen(
                [sys.executable, '-m', 'agent.jobs.worker'],
                start_new_session=True, env=environment))
        children.append(subprocess.Popen(api_command(debug='--debug' in sys.argv),
                                         start_new_session=True, env=environment))
        while not stopping:
            if any(child.poll() is not None for child in children):
                result = 1  # Let the container orchestrator restart the service.
                break
            time.sleep(0.5)
    finally:
        stop_children(children)
    return result


if __name__ == '__main__':
    sys.exit(main())
