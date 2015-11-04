#!/usr/bin/env python
""" responses_recvr is the process that asynchronously receives POOL responses
"""
import argparse
import subprocess
import sys
import json
import time
import threading
from socket import gethostname


from bashtasks.rabbit_util import connect_and_declare
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL

pending_msgs_nr = -1  # pending_msgs_nr: -1 is infinite.


def currtimemillis():
    return int(round(time.time() * 1000))


def handle_response(ch, method, properties, body):
    curr_th_name = threading.current_thread().name
    body_str = body.decode('utf-8')
    msg = json.loads(body_str)
    print(">>>> response received: ", curr_th_name, "from queue ", TASK_RESPONSES_POOL,
          " correlation_id: ", msg['correlation_id'])

    total_time = currtimemillis() - msg['request_ts']
    command_time = msg['post_command_ts'] - msg['pre_command_ts']

    print('--- total_time: {}ms command_time {}ms '.format(total_time, command_time))

    print("<<<< response processed: ", curr_th_name, "response is:", msg)
    count_msg_processed()

    if processed_all_messages():
        print("Processed all messages... exiting.")
        sys.exit()
    # ch.basic_ack(method.delivery_tag)


def start_responses_recvr(host='127.0.0.1', usr='guest', pas='guest'):
    curr_th_name = threading.current_thread().name
    print(">> Starting receiver", curr_th_name, "connecting to rabbitmq:", host, usr, pas)
    consumer_channel = connect_and_declare(host=host, usr=usr, pas=pas)
    consumer_channel.basic_qos(prefetch_count=1)  # consume msgs one at a time
    consumer_channel.basic_consume(handle_response, queue=TASK_RESPONSES_POOL, no_ack=False)
    print("<< Ready: receiver", curr_th_name, "connected to rabbitmq:", host, usr, pas)
    consumer_channel.start_consuming()


def count_msg_processed():
    global pending_msgs_nr
    pending_msgs_nr -= 1


def processed_all_messages():
    global pending_msgs_nr
    if pending_msgs_nr == -1:
        return False
    else:
        return pending_msgs_nr == 0


def set_msgs_to_process(n):
    global pending_msgs_nr
    if n == 0:
        pending_msgs_nr = -1
    else:
        pending_msgs_nr = n


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=globals()['__doc__'], add_help=True)
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=5672, dest='port', type=int)
    parser.add_argument('--user', default='guest', dest='usr')
    parser.add_argument('--pass', default='guest', dest='pas')
    parser.add_argument('--workers', default=1, dest='workers', type=int)
    parser.add_argument('--msgs-nr', default=-1, dest='msgs_nr', type=int)
    args = parser.parse_args()

    set_msgs_to_process(args.msgs_nr)

    for x in range(0, args.workers):
        worker_th = threading.Thread(target=start_responses_recvr,
                                     args=(args.host, args.usr, args.pas),
                                     name='worker_th_' + str(x))
        worker_th.start()
