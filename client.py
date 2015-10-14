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

msg = {
    'command': arguments[1:],
    'correlation_id': currtimemillis(),
    'request_ts': currtimemillis(),
    'reply_to': 'bashrabbit-responses'
}

msg_str = json.dumps(msg)

ch.basic_publish(exchange='bashrabbit', routing_key='', body=msg_str)

times_wout_msg = 0
while times_wout_msg < 10:
    method_frame, header_frame, body = ch.basic_get('bashrabbit-responses')
    if body is not None:
        times_wout_msg = 0
        response_msg = json.loads(body)
        print "received response: returncode :", response_msg['returncode']
        print "                   command    :", ' '.join(response_msg['command'])
        print "                   request_ts :", response_msg['request_ts'], type(response_msg['request_ts'])
        print "                   stdout     :"
        print response_msg['stdout']
        print "Time:"
        print "     total   : ", currtimemillis() - response_msg['request_ts'], "ms."
        print "     command : ", response_msg['post_command_ts'] - response_msg['pre_command_ts'], "ms."
        ch.basic_ack(method_frame.delivery_tag)
    else:
        time.sleep(0.5)
        times_wout_msg += 1


sys.exit(0)
