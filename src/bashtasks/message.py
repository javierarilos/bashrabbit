import time
import json
from bashtasks.constants import Destination, DestinationNames


def currtimemillis():
    return int(round(time.time() * 1000))


class BashTasksMessage(dict):
    def __init__(self, command=None, reply_to=Destination.responses_pool,
                 max_retries=None, **kwargs):
        self['command'] = command
        if not 'reply_to' in self:
            self['reply_to'] = DestinationNames.get_for(reply_to)
        if kwargs:
            self.update(kwargs)
        self.lazy_init_ts('correlation_id')
        self.lazy_init_ts('request_ts')
        if max_retries:
            self['max_retries'] = max_retries

    def to_json(self):
        return json.dumps(self)

    def lazy_init_ts(self, ts_name):
        if not ts_name in self:
            self[ts_name] = currtimemillis()


def get_request(command, reply_to=Destination.responses_pool, max_retries=None):
    return BashTasksMessage(command=command, reply_to=reply_to, max_retries=max_retries)


def from_str(json_str):
    d = json.loads(json_str)
    return BashTasksMessage(**d)
