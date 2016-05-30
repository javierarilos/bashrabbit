#!/usr/bin/env python
""" responses_recvr is the process that asynchronously receives POOL responses
"""
import argparse
import subprocess
import sys
import os
import json
import time
import threading
from socket import gethostname

from bashtasks.rabbit_util import connect_and_declare

from bashtasks import TaskStatistics
from bashtasks import init_subscriber
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL
from bashtasks.logger import get_logger

pending_tasks = -1  # pending_tasks: -1 is infinite.
index = 1  # index to differenciate same correlation_id msgs
stats = None


def curr_module_name():
    return os.path.splitext(os.path.basename(__file__))[0]


def currtimemillis():
    return int(round(time.time() * 1000))


def is_error(msg):
    return msg['returncode'] != 0


def trace_msg(msgs_dir, msg):
    global index
    filename = '{}.{}.msg.json'.format(msg['correlation_id'], index)
    index += 1

    with open(os.path.join(msgs_dir, filename), 'w') as err_file:
        err_file.write(json.dumps(msg))


def log_exc(txt):
    logger = get_logger(name=curr_module_name())
    logger.error(txt)


def init_dir(directory):
    if directory:
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno == 17:  # File exists
                return
            log_exc('Exception in init_dir {} : {}'.format(directory, repr(e)))
        except Exception as e:
            log_exc('Exception in init_dir {} : {}'.format(directory, repr(e)))


def start_responses_recvr(host='127.0.0.1', port=5672, usr='guest', pas='guest', stats=None,
                          msgs_dir=None, trace_err_only=False, verbose=False):
    logger = get_logger(name=curr_module_name())

    def count_message_processed():
        global pending_tasks
        pending_tasks = -1 if pending_tasks == -1 else pending_tasks - 1

        if pending_tasks == 0:
            logger.info("Processed all messages... exiting.")
            stats.sumaryPrettyPrint()
            stats.closeCsvFile()
            sys.exit()
        else:  # pending_tasks < 0 -> infinite. > 0 is the nr msgs pending
            logger.debug('-- still msgs_pending: %d', get_pending_nr())

    def handle_response(response_msg):
        msg = json.loads(response_msg.body.decode('utf-8'))
        logger.debug(">>>> response received: %s from queue %s correlation_id: %d \
                      pending_msgs: %d is_error: %s",
                     threading.current_thread().name, TASK_RESPONSES_POOL,
                     msg['correlation_id'], get_pending_nr(), str(is_error(msg)))
        if verbose:
            logger.info('---------------------------------------- MSG:')
            for key, value in msg.items():
                logger.info('\t%s:-> %s', key, str(value))
            logger.info('---------------------------------------------')

        stats.trackMsg(msg)

        if msgs_dir and (not trace_err_only or is_error(msg)):
            trace_msg(msgs_dir, msg)

        response_msg.ack()

        count_message_processed()

    curr_th_name = threading.current_thread().name

    logger.info(">> Starting receiver %s connecting to rabbitmq: %s:%s@%s",
                curr_th_name, usr, pas, host)

    subscriber = init_subscriber(host=host, port=port, usr=usr, pas=pas)
    subscriber.subscribe(handle_response)


def set_msgs_to_process(n):
    global pending_tasks
    if n == 0:  # infinte
        pending_tasks = -1
    else:
        pending_tasks = n


def get_pending_nr():
    global pending_tasks
    return pending_tasks


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=globals()['__doc__'], add_help=True)
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=5672, dest='port', type=int)
    parser.add_argument('--user', default='guest', dest='usr')
    parser.add_argument('--pass', default='guest', dest='pas')
    parser.add_argument('--workers', default=1, dest='workers', type=int)
    parser.add_argument('--tasks', default=-1, dest='tasks', type=int)
    parser.add_argument('--stats-interval', default=0, dest='stats_interval', type=int)
    parser.add_argument('--csv', default=None, dest='stats_csv_filename')
    parser.add_argument('--msgs-dir', default=None, dest='msgs_dir')
    parser.add_argument('--trace-err-only', action='store_true', dest='trace_err_only')
    parser.add_argument('--verbose', action='store_true', dest='verbose')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    set_msgs_to_process(args.tasks)

    csvAutoSave = args.stats_csv_filename is not None

    if args.msgs_dir:
        init_dir(args.msgs_dir)
    if args.stats_csv_filename:
        init_dir(os.path.dirname(args.stats_csv_filename))

    global stats
    stats = TaskStatistics(csvAuto=csvAutoSave, csvFileName=args.stats_csv_filename)

    if args.stats_interval > 0:  # print stats every stats_interval seconds
        logger = get_logger(name=curr_module_name())
        logger.info('>>>>> args.stats_interval: %d', args.stats_interval)

        def print_stats_every(interval):
            while True:
                time.sleep(interval)
                stats.sumaryPrettyPrint()

        # daemon=True is not supported by python 2.7
        stats_th = threading.Thread(target=print_stats_every,
                                    args=(args.stats_interval,),
                                    name='stats_th')
        stats_th.daemon = True
        stats_th.start()

    for x in range(0, args.workers):
        worker_th = threading.Thread(target=start_responses_recvr,
                                     args=(args.host, args.usr, args.pas, stats,
                                           args.msgs_dir, args.trace_err_only,
                                           args.verbose),
                                     name='worker_th_' + str(x))
        worker_th.start()
