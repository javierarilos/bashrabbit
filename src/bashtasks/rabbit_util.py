from pika import BlockingConnection, ConnectionParameters, BasicProperties, PlainCredentials
from bashtasks.constants import TASK_POOL, TASK_RESPONSES_POOL


def connect(host='localhost', usr='guest', pas='guest'):
    try:
        credentials = PlainCredentials(usr, pas)
        parameters = ConnectionParameters(host, 5672, '/', credentials)
        conn = BlockingConnection(parameters)
    except Exception as e:
        conn = None
    return conn


def connect_and_declare(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    ch = conn.channel()

    ch.exchange_declare(exchange=TASK_POOL, type='direct')
    ch.queue_declare(queue=TASK_POOL)
    ch.queue_bind(exchange=TASK_POOL, queue=TASK_POOL, routing_key='')

    ch.exchange_declare(exchange=TASK_RESPONSES_POOL, type='direct')
    ch.queue_declare(queue=TASK_RESPONSES_POOL)
    ch.queue_bind(exchange=TASK_RESPONSES_POOL, queue=TASK_RESPONSES_POOL, routing_key='')
    return ch


def purge(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    ch = conn.channel()

    ch.queue_purge(queue=TASK_POOL)
    ch.queue_purge(queue=TASK_RESPONSES_POOL)


def is_rabbit_available(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    if conn is not None:
        conn.close()
        return True
    else:
        return False
