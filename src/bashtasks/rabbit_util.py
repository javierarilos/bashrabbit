from pika import BlockingConnection, ConnectionParameters, BasicProperties, PlainCredentials
import pika
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL
from bashtasks.logger import get_logger
import os


def curr_module_name():
    return os.path.splitext(os.path.basename(__file__))[0]


def connect(host='localhost', usr='guest', pas='guest'):
    logger = get_logger(name=curr_module_name())
    try:
        logger.info('Connecting to rabbit: %s:%s@%s', usr, pas, host)
        credentials = PlainCredentials(usr, pas)
        parameters = ConnectionParameters(host, 5672, '/', credentials)
        conn = BlockingConnection(parameters)
    except Exception as e:
        logger.error('Exception connecting to rabbit: %s:%s@%s', usr, pas, host, exc_info=True)
        conn = None
    return conn


def close_channel_and_conn(ch):
    if ch.is_open:
        ch.close()
    if ch._impl.is_open:
        ch._impl.close()


def declare_and_bind(ch, name, routing_key=''):
    ch.exchange_declare(exchange=name, type='topic')
    ch.queue_declare(queue=name)
    ch.queue_bind(exchange=name, queue=name, routing_key=routing_key)


def connect_and_declare(host='localhost', usr='guest', pas='guest', destinations=None):
    """ connects to RabbitMQ and does queue/exchange declarations
        destinations: name(s) of destinations, can be str or list
    """
    if not destinations:
        destinations = [TASK_REQUESTS_POOL, TASK_RESPONSES_POOL]
    elif isinstance(destinations, basestring):
        destinations = [destinations]

    conn = connect(host=host, usr=usr, pas=pas)
    ch = conn.channel()

    for destination in destinations:
        try:
            declare_and_bind(ch, destination, routing_key='#')
        except pika.exceptions.ChannelClosed as e:
            logger = get_logger(name=curr_module_name())
            logger.warning('Destination with name=%s already exists, error. Skipping',
                           destination, exc_info=True)
            conn = connect(host=host, usr=usr, pas=pas)
            ch = conn.channel()

    return ch


def purge(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    ch = conn.channel()
    try:
        ch.queue_purge(queue=TASK_REQUESTS_POOL)
        ch.queue_purge(queue=TASK_RESPONSES_POOL)
        ch.close()
        conn.close()
    except pika.exceptions.ChannelClosed as e:
        print('Not an error if this is a Test. Purging queues exception:', e)


def is_rabbit_available(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    if conn is not None:
        conn.close()
        return True
    else:
        return False
