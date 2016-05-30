from bashtasks.rabbit_util import connect_and_declare, declare_and_bind, close_channel_and_conn
from bashtasks.constants import TASK_RESPONSES_POOL

#TODO: should be removed. rabbit_util should have conn factory, with singleton option
channel_inst = None


class ResponseMsg:
    def __init__(self, ch, method, properties, body):
        self.ch = ch
        self.method = method
        self.properties = properties
        self.body = body

    def ack(self):
        self.ch.basic_ack(self.method.delivery_tag)


def rabbit_msg_received(callback, ch, method, properties, body):
    callback(ResponseMsg(ch, method, properties, body))


class MessageAmqpPika:
    def __init__(self, ch, method, properties, body):
        self._channel = ch
        self._method = method
        self.properties = properties
        self.body = body

    def ack(self):
        self._channel.basic_ack(self._method.delivery_tag)

    def requeue(self):
        raise Exception('MessageAmqpPika.requeue not yet implemented')

    def discard(self):
        raise Exception('MessageAmqpPika.discard not yet implemented')


class TaskResponseSubscriber:
    def __init__(self, host='127.0.0.1', port=5672, usr='guest', pas='guest'):
        self.host = host
        self.port = port
        self.usr = usr
        self.pas = pas

    def subscribe(self, callback, queue=TASK_RESPONSES_POOL):
        def pika_event_to_bashtasks_msg(ch, method, properties, body):
            msg = MessageAmqpPika(ch, method, properties, body)
            callback(msg)

        global channel_inst
        if not channel_inst:
            channel_inst = connect_and_declare(host=self.host, port=self.port, usr=self.usr, pas=self.pas)

        channel_inst.basic_qos(prefetch_count=1)  # consume msgs one at a time

        channel_inst.basic_consume(pika_event_to_bashtasks_msg, queue=queue, no_ack=False)

        channel_inst.start_consuming()


def init_subscriber(host='127.0.0.1', port=5672, usr='guest', pas='guest', channel=None):
    if channel:
        global channel_inst
        channel_inst = channel

    resp_subs = TaskResponseSubscriber(host=host, port=port, usr=usr, pas=pas)
    return resp_subs
