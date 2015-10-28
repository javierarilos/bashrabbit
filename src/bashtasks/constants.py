"""bastasks constants are here
"""
from enum import Enum
from os import getpid
from socket import gethostname


BASHTASKS = 'bashtasks'
TASK_RESPONSES_POOL = 'bashtasks:pool:responses'
TASK_REQUESTS_POOL = 'bashtasks:pool:requests'
RESPONSES = 'responses'
REQUESTS = 'requests'


class Destination(Enum):
    responses_pool = 1
    responses_exclusive = 2
    requests_pool = 3
    requests_exclusive = 3


class DestinationNames:
    destination_providers = {
        Destination.responses_pool: lambda: TASK_RESPONSES_POOL,
        Destination.responses_exclusive: lambda: ':'.join((BASHTASKS, str(getpid()),
                                                     gethostname(), RESPONSES)),
        Destination.requests_pool: lambda: TASK_REQUESTS_POOL,
        Destination.responses_exclusive: lambda: ':'.join((BASHTASKS, str(getpid()),
                                                     gethostname(), REQUESTS))
    }

    @classmethod
    def get_for(cls, destination_type):
        return cls.destination_providers[destination_type]()
