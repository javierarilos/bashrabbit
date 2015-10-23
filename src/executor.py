import subprocess
import sys
import json
import time
from socket import gethostname
from bashtasks.rabbit_util import connect_and_declare
from bashtasks.constants import TASK_POOL, TASK_RESPONSES_POOL

arguments = sys.argv[1:]

host = '127.0.0.1'
usr = 'guest'
pas = 'guest'

arguments_nr = len(arguments)
if arguments_nr > 1 and arguments_nr != 3:
    print "-Usage: python executor.py [host [user password]]."
    sys.exit(1)
if arguments_nr >= 1:
    host = arguments[0]
if arguments_nr == 3:
    usr = arguments[1]
    pas = arguments[2]

print ">> Starting executor connecting to rabbitmq:", host, usr, pas
consumer_channel = connect_and_declare(host=host, usr=usr, pas=pas)


def currtimemillis():
    return int(round(time.time() * 1000))


def handle_command_request(ch, method, properties, body):
        msg = json.loads(body)
        print ">>>> msg received from queue 'bashtasks-jobs' : ", msg
        command = msg[u'command']
        pre_command_ts = currtimemillis()
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        o, e = p.communicate()
        post_command_ts = currtimemillis()

        response_msg = {
            'correlation_id': msg['correlation_id'],
            'reply_to': msg['reply_to'],
            'command': msg['command'],
            'request_ts': msg['request_ts'],
            'pre_command_ts': pre_command_ts,
            'post_command_ts': post_command_ts,
            'returncode': p.returncode,
            'executor_name': gethostname(),
            'stdout': o,
            'stderr': e
        }

        response_str = json.dumps(response_msg)
        print "<<<< executed! response is:", response_msg

        # ch.basic_publish(exchange=msg['reply_to'], routing_key='bashtasks', body=response_str)
        ch.basic_publish(exchange=TASK_RESPONSES_POOL, routing_key='', body=response_str)
        ch.basic_ack(method.delivery_tag)

consumer_channel.basic_consume(handle_command_request, queue=TASK_POOL, no_ack=False)
print "<< Ready: executor connected to rabbitmq:", host, usr, pas
consumer_channel.start_consuming()

sys.exit(0)
