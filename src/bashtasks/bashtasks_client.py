"""bashtasks implementation module
"""
import json
import time
from constants import RESPONSES_POOL, REQUESTS_POOL
from bashtasks.rabbit_util import connect_and_declare

channel_inst = None


def currtimemillis():
    return int(round(time.time() * 1000))


def post_task(command, reply_to=RESPONSES_POOL):
    print '>>>>> posting task ', command
    msg = {
        'command': command,
        'correlation_id': currtimemillis(),
        'request_ts': currtimemillis(),
        'reply_to': reply_to
    }
    msg_str = json.dumps(msg)
    channel_inst.basic_publish(exchange=REQUESTS_POOL, routing_key='', body=msg_str)


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
    return bashtasks


def reset():
    global channel_inst
    if channel_inst is not None:
        channel_inst.close()
        channel_inst = None
