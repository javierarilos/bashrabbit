from pika import BlockingConnection, ConnectionParameters, BasicProperties, PlainCredentials


def connect_and_declare(host='localhost', usr='guest', pas='guest'):
    credentials = PlainCredentials(usr, pas)
    parameters = ConnectionParameters(host, 5672, '/', credentials)
    conn = BlockingConnection(parameters)
    # conn = BlockingConnection(ConnectionParameters(host))
    ch = conn.channel()

    ch.exchange_declare(exchange='bashtasks', type='direct')
    ch.queue_declare(queue='bashtasks-jobs')
    ch.queue_bind(exchange='bashtasks', queue='bashtasks-jobs', routing_key='')

    ch.exchange_declare(exchange='bashtasks-responses', type='direct')
    ch.queue_declare(queue='bashtasks-responses')
    ch.queue_bind(exchange='bashtasks-responses', queue='bashtasks-responses', routing_key='')
    return ch
