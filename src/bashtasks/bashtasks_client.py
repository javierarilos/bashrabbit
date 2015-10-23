"""bashtasks implementation module
"""
import json
import time
from datetime import datetime

from constants import TASK_RESPONSES_POOL, TASK_POOL
from bashtasks.rabbit_util import connect_and_declare

channel_inst = None


def currtimemillis():
    return int(round(time.time() * 1000))


def post_task(command, reply_to=TASK_RESPONSES_POOL):
    """ posts command to executors via RabbitMQ TASK_POOL
        does NOT wait for response.
        :return: <dict> message created for the task.
    """
    print '>>>>> posting task ', command
    msg = {
        'command': command,
        'correlation_id': currtimemillis(),
        'request_ts': currtimemillis(),
        'reply_to': reply_to
    }
    msg_str = json.dumps(msg)
    channel_inst.basic_publish(exchange=TASK_POOL, routing_key='', body=msg_str)
    return msg


def execute_task(command, reply_to=TASK_RESPONSES_POOL, timeout=10):
    """ posts command to executors via RabbitMQ TASK_POOL
        synchronously waits for response.
        :return: <dict> response message.
    """
    task = post_task(command, reply_to)
    start_waiting = datetime.now()
    while True:
        method_frame, header_frame, body = channel_inst.basic_get(TASK_RESPONSES_POOL)
        if body:
            channel_inst.basic_ack(method_frame.delivery_tag)
            return json.loads(body)
        else:
            if (datetime.now() - start_waiting).total_seconds() > timeout:
                raise Exception('Timeout ({}secs) waiting for response 2 msg: {} in queue: "{}"'
                                .format(timeout, task['correlation_id'], reply_to))
            time.sleep(0.01)


class BashTasks:
    pass


def init(host='127.0.0.1', usr='guest', pas='guest', channel=None):
    global channel_inst
    if not channel:
        channel_inst = connect_and_declare(host=host, usr=usr, pas=pas)
    else:
        channel_inst = channel
    print('channel_inst', channel_inst)

    bashtasks = BashTasks()
    bashtasks.post_task = post_task
    bashtasks.execute_task = execute_task
    return bashtasks


def reset():

    global channel_inst
    if channel_inst is not None:
        channel_inst.close()
        channel_inst = None
