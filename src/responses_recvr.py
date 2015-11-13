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
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL

pending_tasks = -1  # pending_tasks: -1 is infinite.
stats = None


def currtimemillis():
    return int(round(time.time() * 1000))


def is_error(msg):
    return msg['returncode'] != 0


def trace_error_msg(err_dir, msg):
    filename = '{}.msg.err.txt'.format(msg['correlation_id'])

    with open(os.path.join(err_dir, filename), 'w') as err_file:
        err_file.write(json.dumps(msg))


def init_dir(dir):
    if dir:
        os.makedirs(dir, exist_ok=True)


def start_responses_recvr(host='127.0.0.1', usr='guest', pas='guest', stats=None, err_dir=None):
    init_dir(err_dir)

    def count_message_processed():
        global pending_tasks
        pending_tasks = -1 if pending_tasks == -1 else pending_tasks - 1

        if pending_tasks == 0:
            print("Processed all messages... exiting.")
            stats.sumaryPrettyPrint()
            stats.closeCsvFile()
            sys.exit()
        else:  # pending_tasks < 0 -> infinite. > 0 is the nr msgs pending
            print('-- still msgs_pending:', get_pending_nr())

    def handle_response(ch, method, properties, body):
        msg = json.loads(body.decode('utf-8'))
        print(">>>> response received: ", threading.current_thread().name,
              "from queue ", TASK_RESPONSES_POOL,
              " correlation_id: ", msg['correlation_id'],
              " pending_msgs: ", get_pending_nr(),
              " is_error: ", str(is_error(msg)))

        stats.trackMsg(msg)

        if err_dir and is_error(msg):
            print('-------------------- MSG IS ERROR:')
            print(json.dumps(msg, indent=4, separators=(',', ': ')))
            print('--------------------')
            trace_error_msg(err_dir, msg)

        ch.basic_ack(method.delivery_tag)

        count_message_processed()

    curr_th_name = threading.current_thread().name
    print(">> Starting receiver", curr_th_name, "connecting to rabbitmq:", host, usr, pas)
    consumer_channel = connect_and_declare(host=host, usr=usr, pas=pas)
    consumer_channel.basic_qos(prefetch_count=1)  # consume msgs one at a time
    consumer_channel.basic_consume(handle_response, queue=TASK_RESPONSES_POOL, no_ack=False)
    print("<< Ready: receiver", curr_th_name, "connected to rabbitmq:", host, usr, pas)
    consumer_channel.start_consuming()


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
    parser.add_argument('--err-dir', default=None, dest='err_dir')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    set_msgs_to_process(args.tasks)

    csvAutoSave = args.stats_csv_filename is not None

    global stats
    stats = TaskStatistics(csvAuto=csvAutoSave, csvFileName=args.stats_csv_filename)

    if args.stats_interval > 0:  # print stats every stats_interval seconds
        print('>>>>> args.stats_interval', args.stats_interval)

        def print_stats_every(interval):
            while True:
                time.sleep(interval)
                stats.sumaryPrettyPrint()

        stats_th = threading.Thread(target=print_stats_every,
                                    args=(args.stats_interval,),
                                    name='stats_th',
                                    daemon=True)
        stats_th.start()

    for x in range(0, args.workers):
        worker_th = threading.Thread(target=start_responses_recvr,
                                     args=(args.host, args.usr, args.pas, stats, args.err_dir),
                                     name='worker_th_' + str(x))
        worker_th.start()
