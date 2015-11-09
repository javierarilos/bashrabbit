#!/usr/bin/env python
""" executor is the process that executes bashtasks
"""
import argparse
import subprocess
import sys
import json
import time
import threading
from socket import gethostname


from bashtasks.rabbit_util import connect_and_declare, close_channel_and_conn
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL


def currtimemillis():
    return int(round(time.time() * 1000))


def start_executor(host='127.0.0.1', usr='guest', pas='guest', tasks_nr=1):
    def handle_command_request(ch, method, properties, body):
        curr_th_name = threading.current_thread().name
        body_str = body.decode('utf-8')
        msg = json.loads(body_str)
        print(">>>> msg received: ", curr_th_name, "from queue ", TASK_REQUESTS_POOL, " : correlation_id", msg['correlation_id'], "command: ", msg['command'])
        command = msg[u'command']
        pre_command_ts = currtimemillis()
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        o, e = p.communicate()
        post_command_ts = currtimemillis()

        response_msg = {
            'correlation_id': msg['correlation_id'],
            'reply_to': msg['reply_to'],
            'command': msg['command'],
            'request_ts': msg['request_ts'],
            'pre_command_ts': pre_command_ts,
            'post_command_ts': post_command_ts,
            'returncode': p.returncode,
            'executor_name': gethostname(),
            'stdout': o.decode('utf-8'),
            'stderr': e.decode('utf-8')
        }

        response_str = json.dumps(response_msg)

        ch.basic_publish(exchange=TASK_RESPONSES_POOL, routing_key='', body=response_str)
        ch.basic_ack(method.delivery_tag)

        nonlocal tasks_nr
        tasks_nr = tasks_nr - 1 if tasks_nr > 0 else tasks_nr
        if response_msg['returncode'] != 0:
            print('************************************************************ ERR ', response_msg['correlation_id'])
            print('returncode:', response_msg['returncode'])
            print('stdout:', response_msg['stdout'])
            print('stderr:', response_msg['stderr'])
            print('************************************************************')
        print("<<<< executed by: executor", curr_th_name, "correlation_id:",
              response_msg['correlation_id'], "pending:", tasks_nr)
        if tasks_nr == 0:
            print('==== no more tasks to execute. Exiting.')
            close_channel_and_conn(ch)
            sys.exit(0)

    curr_th_name = threading.current_thread().name
    print(">> Starting executor", curr_th_name, "connecting to rabbitmq:", host, usr, pas,
          "executing", tasks_nr, "tasks.")
    consumer_channel = connect_and_declare(host=host, usr=usr, pas=pas)
    consumer_channel.basic_qos(prefetch_count=1)  # consume msgs one at a time
    consumer_channel.basic_consume(handle_command_request, queue=TASK_REQUESTS_POOL, no_ack=False)
    print("<< Ready: executor", curr_th_name, "connected to rabbitmq:", host, usr, pas)
    consumer_channel.start_consuming()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=globals()['__doc__'], add_help=True)
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=5672, dest='port', type=int)
    parser.add_argument('--user', default='guest', dest='usr')
    parser.add_argument('--pass', default='guest', dest='pas')
    parser.add_argument('--workers', default=1, dest='workers', type=int)
    parser.add_argument('--tasks', default=-1, dest='tasks_nr', type=int)
    args = parser.parse_args()
    worker_ths = []
    for x in range(0, args.workers):
        worker_th = threading.Thread(target=start_executor,
                                     args=(args.host, args.usr, args.pas, args.tasks_nr),
                                     name='worker_th_' + str(x),
                                     daemon=True)
        worker_th.start()
        worker_ths.append(worker_th)

    for worker_th in worker_ths:
        worker_th.join()
