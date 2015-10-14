import subprocess
import sys
import json
import time
from socket import gethostname
from rabbit_util import connect_and_declare

consumer_channel = connect_and_declare()

def handle_command_request(ch, method, properties, body):
        msg = json.loads(body)
        print ">>>> msg received from queue 'bashrabbit-jobs' : ", msg
        command = msg[u'command']

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        o, e = p.communicate()

        response_msg = {
            'correlation_id': msg[u'correlation_id'],
            'reply_to': msg[u'reply_to'],
            'command': msg[u'command'],
            'returncode': p.returncode,
            'executor_name': gethostname(),
            'stdout': o,
            'stderr': e
        }

        response_str = json.dumps(response_msg)
        print "<<<< executed! response is:", response_msg

        #ch.basic_publish(exchange=msg['reply_to'], routing_key='bashrabbit', body=response_str)
        ch.basic_publish(exchange='bashrabbit-responses', routing_key='', body=response_str)
        ch.basic_ack(method.delivery_tag)

method_frame, header_frame, body = consumer_channel.basic_get('bashrabbit-jobs')

consumer_channel.basic_consume(handle_command_request, queue='bashrabbit-jobs', no_ack=False)
consumer_channel.start_consuming()

sys.exit(0)
