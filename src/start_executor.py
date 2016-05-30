#!/usr/bin/env python
""" start_executor is the process that executes bashtasks
"""

import argparse
import sys

from bashtasks.constants import DestinationNames, TASK_REQUESTS_POOL
from bashtasks.executor import register_signals_handling, start_executors, curr_module_name
from bashtasks.logger import get_logger

channels = []  # stores all executor thread channels.
stop = False  # False until the executor is asked to stop
MB_10 = 10485760

DEFAULT_DESTINATION = DestinationNames.get_for(TASK_REQUESTS_POOL)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=globals()['__doc__'], add_help=True)
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=5672, dest='port', type=int)
    parser.add_argument('--user', default='guest', dest='usr')
    parser.add_argument('--pass', default='guest', dest='pas')
    parser.add_argument('--workers', default=1, dest='workers', type=int)
    parser.add_argument('--tasks', default=-1, dest='tasks_nr', type=int)
    parser.add_argument('--max-retries', default=0, dest='max_retries', type=int)
    parser.add_argument('--verbose', action='store_true', dest='verbose')
    parser.add_argument('--queue', default=DEFAULT_DESTINATION, dest='queue')

    register_signals_handling()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    start_executors(args.workers, args.host, args.port, args.usr, args.pas,  queue=args.queue,
                    tasks_nr=args.tasks_nr, max_retries=args.max_retries, verbose=args.verbose)
    get_logger(name=curr_module_name()).info('Executor exiting now.')
