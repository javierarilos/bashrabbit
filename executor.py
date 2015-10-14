import subprocess
import sys
import json
import time
from socket import gethostname
from rabbit_util import connect_and_declare

ch = connect_and_declare()

while True:
    method_frame, header_frame, body = ch.basic_get('bashrabbit-jobs')
    if body is not None:
        msg = json.loads(body)
        print "msg received from queue 'bashrabbit-jobs' : ", msg
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
        print "executed! response is:", response_msg

        #ch.basic_publish(exchange=msg['reply_to'], routing_key='bashrabbit', body=response_str)
        ch.basic_publish(exchange='bashrabbit-responses', routing_key='', body=response_str)
        ch.basic_ack(method_frame.delivery_tag)
    else:
        print "no message was waiting for us... sleep."
        time.sleep(1)

sys.exit(0)
