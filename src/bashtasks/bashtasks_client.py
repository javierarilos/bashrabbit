"""bashtasks implementation module
"""
import json
import time
from datetime import datetime
import pika

from bashtasks.constants import TASK_RESPONSES_POOL, TASK_REQUESTS_POOL
from bashtasks.constants import Destination, DestinationNames
from bashtasks.rabbit_util import connect_and_declare, declare_and_bind, close_channel_and_conn
from bashtasks import message

channel_inst = None
DEFAULT_DESTINATION = DestinationNames.get_for(TASK_REQUESTS_POOL)

def currtimemillis():
    return int(round(time.time() * 1000))


def post_task(command, destination=DEFAULT_DESTINATION, reply_to=Destination.responses_pool, max_retries=None, non_retriable=[]):
    """ posts command to executors via RabbitMQ TASK_REQUESTS_POOL
        does NOT wait for response.
        :return: <dict> message created for the task.
    """
    msg = message.get_request(command, reply_to=Destination.responses_pool, max_retries=max_retries, non_retriable=non_retriable)

    if reply_to is Destination.responses_exclusive:
        declare_and_bind(channel_inst, msg['reply_to'])

    msg_str = msg.to_json()
    props = pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      )
    channel_inst.basic_publish(exchange=destination, routing_key='', body=msg_str, properties=props)
    return msg


def execute_task(command, destination=DEFAULT_DESTINATION, reply_to=Destination.responses_pool, timeout=10, max_retries=None, non_retriable=[]):
    """ posts command to executors via RabbitMQ TASK_REQUESTS_POOL
        synchronously waits for response.
        :return: <dict> response message.
    """
    task = post_task(command, destination=destination, reply_to=reply_to, max_retries=max_retries, non_retriable=non_retriable)
    start_waiting = datetime.now()
    while True:
        method_frame, header_frame, body = channel_inst.basic_get(TASK_RESPONSES_POOL)
        if body:
            channel_inst.basic_ack(method_frame.delivery_tag)
            return message.from_str(body.decode('utf-8'))
        else:
            if (datetime.now() - start_waiting).total_seconds() > timeout:
                raise Exception('Timeout ({}secs) waiting for response to msg: {} in queue: "{}"'
                                .format(timeout, task['correlation_id'], reply_to))
            time.sleep(0.01)


class BashTasks:
    pass


def init(host='127.0.0.1', usr='guest', pas='guest', channel=None):
    global channel_inst
    if not channel:
        #TODO should lazy init channel_inst
        channel_inst = connect_and_declare(host=host, usr=usr, pas=pas)
    else:
        channel_inst = channel

    bashtasks = BashTasks()
    bashtasks.post_task = post_task
    bashtasks.execute_task = execute_task
    return bashtasks


def reset():

    global channel_inst
    if channel_inst is not None:
        close_channel_and_conn(channel_inst)
        channel_inst = None
