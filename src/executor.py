#!/usr/bin/env python
""" executor is the process that executes bashtasks
"""
import signal
import argparse
import subprocess
import sys
import json
import time
import os
import threading
from socket import gethostname
from time import sleep
import logging

from bashtasks.rabbit_util import connect_and_declare, close_channel_and_conn
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL

channels = []  # stores all executor thread channels.
stop = False  # False until the executor is asked to stop
MB_10 = 10485760


def curr_module_name():
    return os.path.splitext(os.path.basename(__file__))[0]


def get_logger():
    logger = logging.getLogger(curr_module_name())
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('{asctime};{name};{threadName};{levelname};{message}',
                                  style='{', datefmt='%Y-%m-%d;%H:%M:%S')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger


def currtimemillis():
    return int(round(time.time() * 1000))


def get_executor_name():
    return ":".join((gethostname(), str(os.getpid()), threading.current_thread().name))


def get_thread_name():
    return 'worker_th_' + str(x)


def start_executor(host='127.0.0.1', usr='guest', pas='guest', tasks_nr=1, max_retries=0):
    curr_th_name = threading.current_thread().name
    logger = get_logger()
    logger.info(">> Starting executor %s connecting to rabbitmq: %s:%s@%s for executing %d tasks.",
                curr_th_name, usr, pas, host, tasks_nr)

    ch = connect_and_declare(host=host, usr=usr, pas=pas)
    ch.basic_qos(prefetch_count=1)  # consume msgs one at a time

    channels.append(ch)

    def create_response_for(msg):
        return {
            'correlation_id': msg['correlation_id'],
            'reply_to': msg['reply_to'],
            'command': msg['command'],
            'request_ts': msg['request_ts'],
            'executor_name': get_executor_name(),
            'retries': msg.get('retries', 0),
            'max_retries': msg.get('max_retries', max_retries)
        }

    def should_retry(response_msg):
        is_error = response_msg['returncode'] != 0
        current_retries = response_msg.get('retries', 0)
        msg_max_retries = response_msg.get('max_retries', max_retries)
        retries_pending = current_retries < msg_max_retries
        return is_error and retries_pending

    def send_response(response_msg):
        if should_retry(response_msg):
            logger.debug('---- retrying msg correlation_id: %d current_retries: %d of %d',
                         response_msg['correlation_id'], response_msg['retries'],
                         response_msg['max_retries'])
            tgt_exch = TASK_REQUESTS_POOL
            response_msg['retries'] += 1
        else:
            tgt_exch = TASK_RESPONSES_POOL

        response_str = json.dumps(response_msg)
        ch.basic_publish(exchange=tgt_exch, routing_key='', body=response_str)

    def tasks_nr_generator(tasks_nr):
        tasks_nr_gen = tasks_nr
        while True:
            tasks_nr_gen = tasks_nr_gen - 1 if tasks_nr_gen > 0 else tasks_nr_gen
            yield tasks_nr_gen

    def handle_command_request(ch, method, properties, body):
        msg = json.loads(body.decode('utf-8'))
        logger.debug(">>>> msg received: %s from queue %s : correlation_id %d command: %s",
                     curr_th_name, TASK_REQUESTS_POOL, msg['correlation_id'], msg['command'])

        response_msg = create_response_for(msg)
        try:
            response_msg['pre_command_ts'] = currtimemillis()

            p = subprocess.Popen(msg['command'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            o, e = p.communicate()

            response_msg['post_command_ts'] = currtimemillis()
            response_msg['returncode'] = p.returncode
            response_msg['stdout'] = o.decode('utf-8')
            response_msg['stderr'] = e.decode('utf-8')

        except Exception as exc:
            logger.error('**** got exception for : correlation_id %d ',
                         response_msg['correlation_id'], exc_info=True)
            response_msg['post_command_ts'] = currtimemillis()
            response_msg['returncode'] = -3791
            response_msg['stdout'] = 'Exception trying to execute command.'
            response_msg['stderr'] = repr(exc)

        finally:
            send_response(response_msg)
            ch.basic_ack(method.delivery_tag)

        tasks_nr_new_elem = next(tasks_nr_gen)
        if response_msg['returncode'] != 0:
            logger.error('**************** ERR %d', response_msg['correlation_id'])
            logger.error('returncode: %d', response_msg['returncode'])
            logger.error('stdout: %s', response_msg['stdout'])
            logger.error('stderr: %s', response_msg['stderr'])
            logger.error('****************')

        logger.debug("<<<< executed by: executor %s correlation_id: %d pending: %d",
                     curr_th_name, response_msg['correlation_id'], tasks_nr_new_elem)

        if tasks_nr_new_elem == 0:
            logger.info('==== no more tasks to execute. Exiting.')
            stop_and_exit()

    tasks_nr_gen = tasks_nr_generator(tasks_nr)
    ch.basic_consume(handle_command_request, queue=TASK_REQUESTS_POOL, no_ack=False)
    logger.info("<< Ready: executor %s connected to rabbitmq: %s:%s@%s",
                curr_th_name, usr, pas, host)
    ch.start_consuming()


def stop_ampq_channels():
    worker_stopping = 1
    logger = get_logger()
    for ch in channels:
        logger.info('\tStopping worker (%d)...', worker_stopping)

        if ch.is_open:
            try:
                ch.close()
            except Exception as e:
                logger.error('Exception closing channel', exc_info=True)
        logger.info('\tStopped worker (%d).', worker_stopping)
        worker_stopping += 1


def stop_and_exit():
    stop_ampq_channels()
    logger = get_logger()
    logger.info('Stopped all AMQP channels.')
    global stop
    stop = True


def register_signals_handling():
    def signal_handler(signal, frame):
        logger = get_logger()
        logger.info('Received signal: %s', str(signal))
        stop_and_exit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=globals()['__doc__'], add_help=True)
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=5672, dest='port', type=int)
    parser.add_argument('--user', default='guest', dest='usr')
    parser.add_argument('--pass', default='guest', dest='pas')
    parser.add_argument('--workers', default=1, dest='workers', type=int)
    parser.add_argument('--tasks', default=-1, dest='tasks_nr', type=int)
    parser.add_argument('--max-retries', default=0, dest='max_retries', type=int)

    register_signals_handling()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    worker_ths = []
    for x in range(0, args.workers):
        # daemon=True is not supported by python 2.7
        worker_th = threading.Thread(target=start_executor,
                                     args=(args.host, args.usr, args.pas,
                                           args.tasks_nr, args.max_retries),
                                     name=get_thread_name())
        worker_th.daemon = True

        worker_th.start()
        worker_ths.append(worker_th)

    while not stop:
        sleep(1)

    get_logger().info('Executor exiting now.')
