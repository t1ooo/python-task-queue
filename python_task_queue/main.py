from argparse import ArgumentParser
from importlib import import_module
import signal
import logging
import sys
from .logger import logger
from .task_queue import TaskQueue
from .shared import checked_cast

logging.basicConfig(level=logging.WARNING)
logging.getLogger("python_task_queue").setLevel(logging.INFO)


def main():
    ap = ArgumentParser()
    ap.add_argument("module", help="path/to/module/module.name:task_queue_instance")
    args = ap.parse_args(sys.argv[1:])

    s = args.module
    s = s.split("/")
    path, module = "/".join(s[:-1]), s[-1]
    module, key = module.split(":", 2)
    logger.info(f"try to import path=`{path}`, module=`{module}`, key=`{key}`")

    if path:
        sys.path.append(path)

    m = import_module(module)
    task_queue = checked_cast(TaskQueue, getattr(m, key))

    def stop(signum=None, frame=None):
        print(signum, frame)
        task_queue.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        task_queue.worker.start()
    except Exception:
        raise
    finally:
        stop()
