from pika import BlockingConnection, ConnectionParameters, BasicProperties, PlainCredentials
import pika
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL


def connect(host='localhost', usr='guest', pas='guest'):
    try:
        credentials = PlainCredentials(usr, pas)
        parameters = ConnectionParameters(host, 5672, '/', credentials)
        conn = BlockingConnection(parameters)
    except Exception as e:
        conn = None
    return conn


def declare_and_bind(ch, name, routing_key=''):
    ch.exchange_declare(exchange=name, type='direct')
    ch.queue_declare(queue=name)
    ch.queue_bind(exchange=name, queue=name, routing_key=routing_key)


def connect_and_declare(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    ch = conn.channel()

    declare_and_bind(ch, TASK_REQUESTS_POOL, routing_key='')
    declare_and_bind(ch, TASK_RESPONSES_POOL, routing_key='')

    return ch


def purge(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    ch = conn.channel()
    try:
        ch.queue_purge(queue=TASK_REQUESTS_POOL)
        ch.queue_purge(queue=TASK_RESPONSES_POOL)
    except pika.exceptions.ChannelClosed as e:
        print('Not an error if this is a Test. Purging queues exception:', e)


def is_rabbit_available(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    if conn is not None:
        conn.close()
        return True
    else:
        return False
