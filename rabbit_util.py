from pika import BlockingConnection, ConnectionParameters, BasicProperties

def connect_and_declare(host='localhost'):
    conn = BlockingConnection(ConnectionParameters(host))
    ch = conn.channel()

    ch.exchange_declare(exchange='bashrabbit', type='direct')
    ch.queue_declare(queue='bashrabbit-jobs')
    ch.queue_bind(exchange='bashrabbit', queue='bashrabbit-jobs', routing_key='')

    ch.exchange_declare(exchange='bashrabbit-responses', type='direct')
    ch.queue_declare(queue='bashrabbit-responses')
    ch.queue_bind(exchange='bashrabbit-responses', queue='bashrabbit-responses', routing_key='')
    return ch
