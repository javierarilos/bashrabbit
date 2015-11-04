import time
from collections import Counter


def currtimemillis():
    return int(round(time.time() * 1000))


def time_post_to_executed(msg):
    return msg['post_command_ts'] - msg['request_ts']


class TaskStatistics:
    def __init__(self):
        self.msgs = []

    def trackMsg(self, msg):
        self.msgs.append(msg)

    def msgsNumber(self):
        return len(self.msgs)

    def allTimesToExecuted(self):
        return (time_post_to_executed(msg) for msg in self.msgs)

    def avgTimeToExecuted(self):
        return sum(self.allTimesToExecuted())//len(self.msgs)

    def maxTimeToExecuted(self):
        return max(self.allTimesToExecuted())

    def allErrors(self):
        return [msg for msg in self.msgs if msg['returncode'] != 0]

    def errorsNumber(self):
        return len(self.allErrors())

    def getWorkersCounter(self):
        return Counter((msg['executor_name'] for msg in self.msgs))

    def getReturnCodesCounter(self):
        return Counter((msg['returncode'] for msg in self.msgs))
