"""bashtasks implementation module
"""
import json
import time
from datetime import datetime

from bashtasks.constants import TASK_RESPONSES_POOL, TASK_REQUESTS_POOL
from bashtasks.constants import Destination, DestinationNames
from bashtasks.rabbit_util import connect_and_declare, declare_and_bind, close_channel_and_conn

channel_inst = None


def currtimemillis():
    return int(round(time.time() * 1000))


def post_task(command, reply_to=Destination.responses_pool, max_retries=None):
    """ posts command to executors via RabbitMQ TASK_REQUESTS_POOL
        does NOT wait for response.
        :return: <dict> message created for the task.
    """

    if reply_to is Destination.responses_exclusive:
        declare_and_bind(channel_inst, DestinationNames.get_for(reply_to))

    msg = {
        'command': command,
        'correlation_id': currtimemillis(),
        'request_ts': currtimemillis(),
        'reply_to': DestinationNames.get_for(reply_to)
    }
    if max_retries:
        msg['max_retries'] = max_retries
    msg_str = json.dumps(msg)
    channel_inst.basic_publish(exchange=TASK_REQUESTS_POOL, routing_key='', body=msg_str)
    return msg


def execute_task(command, reply_to=Destination.responses_pool, timeout=10, max_retries=None):
    """ posts command to executors via RabbitMQ TASK_REQUESTS_POOL
        synchronously waits for response.
        :return: <dict> response message.
    """
    task = post_task(command, reply_to, max_retries=max_retries)
    start_waiting = datetime.now()
    while True:
        method_frame, header_frame, body = channel_inst.basic_get(TASK_RESPONSES_POOL)
        if body:
            channel_inst.basic_ack(method_frame.delivery_tag)
            return json.loads(body.decode('utf-8'))
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
