from pika import BlockingConnection, ConnectionParameters, BasicProperties, PlainCredentials


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

    ch.exchange_declare(exchange='bashtasks', type='direct')
    ch.queue_declare(queue='bashtasks-jobs')
    ch.queue_bind(exchange='bashtasks', queue='bashtasks-jobs', routing_key='')

    ch.exchange_declare(exchange='bashtasks-responses', type='direct')
    ch.queue_declare(queue='bashtasks-responses')
    ch.queue_bind(exchange='bashtasks-responses', queue='bashtasks-responses', routing_key='')
    return ch


def is_rabbit_available(host='localhost', usr='guest', pas='guest'):
    conn = connect(host=host, usr=usr, pas=pas)
    if conn is not None:
        conn.close()
        return True
    else:
        return False
