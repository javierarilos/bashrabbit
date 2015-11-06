import time
from collections import Counter


def currtimemillis():
    return int(round(time.time() * 1000))


def time_post_to_executed(msg):
    return msg['post_command_ts'] - msg['request_ts']


def time_waiting(msg):
    return msg['pre_command_ts'] - msg['request_ts']


def time_executing(msg):
    return msg['post_command_ts'] - msg['pre_command_ts']


class TaskStatistics:
    def __init__(self):
        self.msgs = []
        self.firstMsgTs = 0

    def trackMsg(self, msg):
        if not self.firstMsgTs:
            self.firstMsgTs = currtimemillis()
        self.msgs.append(msg)

    def msgsNumber(self):
        return len(self.msgs)

    def allTimesToExecuted(self):
        return (time_post_to_executed(msg) for msg in self.msgs)

    def avgTimeToExecuted(self):
        aggregated_time_to_executed = sum(self.allTimesToExecuted())
        if aggregated_time_to_executed:
            return aggregated_time_to_executed//len(self.msgs)
        else:
            return 0

    def maxTimeToExecuted(self):
        return max(self.allTimesToExecuted(), default=0)

    def allExecutionTimes(self):
        return (time_executing(msg) for msg in self.msgs)

    def avgExecutionTime(self):
        aggregated_execution_time = sum(self.allExecutionTimes())
        if aggregated_execution_time:
            return aggregated_execution_time//len(self.msgs)
        else:
            return 0

    def maxExecutionTime(self):
        return max(self.allExecutionTimes(), default=0)

    def allTimesWaiting(self):
        return (time_waiting(msg) for msg in self.msgs)

    def avgTimeWaiting(self):
        aggregated_time_waiting = sum(self.allTimesWaiting())
        if aggregated_time_waiting:
            return aggregated_time_waiting//len(self.msgs)
        else:
            return 0

    def allErrors(self):
        return [msg for msg in self.msgs if msg['returncode'] != 0]

    def errorsNumber(self):
        return len(self.allErrors())

    def okNumber(self):
        return self.msgsNumber() - self.errorsNumber()

    def getWorkersCounter(self):
        return Counter((msg['executor_name'] for msg in self.msgs))

    def getReturnCodesCounter(self):
        return Counter((msg['returncode'] for msg in self.msgs))

    def getDuration(self):
        return currtimemillis() - self.firstMsgTs

    def sumaryToPrettyString(self):
        return '\n'.join((
            "________________________________________________________________________________",
            "   Stats after {}ms running:".format(self.getDuration()),
            "        Messages        : {} ({} OK / {} errors)".format(self.msgsNumber(),
                                                                      self.okNumber(),
                                                                      self.errorsNumber()),
            "        Execution time  : {} avg ({} max)".format(self.avgExecutionTime(),
                                                               self.maxExecutionTime()),
            "        Waiting times   : {} avg".format(self.avgTimeWaiting()),
            "        Total task time : {} avg ({} max)".format(self.avgTimeToExecuted(),
                                                               self.maxTimeToExecuted()),
            "________________________________________________________________________________"
        ))

    def sumaryPrettyPrint(self):
        print(self.sumaryToPrettyString())
