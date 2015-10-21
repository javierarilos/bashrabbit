import sys
import json
import time
from rabbit_util import connect_and_declare

ch = connect_and_declare()

arguments = sys.argv

if len(arguments) < 2:
    print "Usage: python client.py <COMMAND> [ARGUMENTS...]"
    sys.exit(0)


def currtimemillis():
    return int(round(time.time() * 1000))

correlation_id = currtimemillis()


def handle_command_response(ch, method, properties, body):
    response_msg = json.loads(body)
    print "received response: returncode :", response_msg['returncode']
    print "                   command    :", ' '.join(response_msg['command'])
    print "                   request_ts :", response_msg['request_ts'],\
        type(response_msg['request_ts']), 'aaaaaaaaaaaaaaaaaaa'
    print "                   stdout     :"
    print response_msg['stdout']
    print "Time:"
    print "     total   : ", currtimemillis() - response_msg['request_ts'], "ms."
    print "     command : ", response_msg['post_command_ts'] - response_msg['pre_command_ts'], "ms."
    ch.basic_ack(method.delivery_tag)
    if response_msg['correlation_id'] == correlation_id:
        print "correlation_id matches. msg is response for sent command. correlation_id:",\
            correlation_id
        ch.stop_consuming()


ch.basic_consume(handle_command_response, queue='bashrabbit-responses', no_ack=False)

msg = {
    'command': arguments[1:],
    'correlation_id': correlation_id,
    'request_ts': currtimemillis(),
    'reply_to': 'bashrabbit-responses'
}
msg_str = json.dumps(msg)
ch.basic_publish(exchange='bashrabbit', routing_key='', body=msg_str)

ch.start_consuming()
sys.exit(0)
