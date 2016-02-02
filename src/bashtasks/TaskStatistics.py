import time
from collections import Counter
from random import random


def currtimemillis():
    return int(round(time.time() * 1000))


def time_post_to_executed(msg):
    return msg['post_command_ts'] - msg['request_ts']


def time_waiting(msg):
    return msg['pre_command_ts'] - msg['request_ts']


def time_executing(msg):
    return msg['post_command_ts'] - msg['pre_command_ts']

csv_fields = (
    "request_ts",
    "correlation_id",
    "pre_command_ts",
    "post_command_ts",
    "returncode",
    "retries",
    "max_retries",
    "executor_name",
    "command",
    "non_retriable"
)

DEFAULT_CSV = 'stats_bashtasks.csv'


class TaskStatistics:
    def __init__(self, csvAuto=False, csvFileName=DEFAULT_CSV, csvPersistenceRatio=0.2):
        self.msgs = []
        self.firstMsgTs = 0
        self.csvAuto = csvAuto
        self.csvFileName = csvFileName
        self.csvFile = None  # lazy initialized
        self.csvPersistenceRatio = csvPersistenceRatio

    @staticmethod
    def csvFields():
        return csv_fields

    def trackMsg(self, msg):
        self.msgs.append(msg)

        if not self.firstMsgTs:
            self.firstMsgTs = currtimemillis()
            if self.csvAuto:
                self.writeCsvHeaders(filepath=self.csvFileName)

        if self.csvAuto:
            self.writeCsvMessage(msg, filepath=self.csvFileName)


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
        if self.allTimesToExecuted() is None or len(list(self.allTimesToExecuted())) is 0:
            return 0
        else:
            return max(self.allTimesToExecuted())

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

    def getCsvFile(self, filepath=None):
        if self.csvFile:
            return self.csvFile

        filepath = filepath if filepath else self.csvFileName

        self.csvFile = open(filepath, 'w')
        return self.csvFile

    def closeCsvFile(self):
        if self.csvFile:
            self.csvFile.close()

    def writeCsvHeaders(self, filepath=None):
        headers = ';'.join((csv_fields)) + '\n'
        f = self.getCsvFile(filepath)
        f.write(headers)

    def writeCsvMessage(self, msg, filepath=None):
        csv_msg = ";".join((str(msg[field]) for field in csv_fields)) + '\n'
        f = self.getCsvFile(filepath)
        f.write(csv_msg)
        if random() < self.csvPersistenceRatio:
            f.flush()

    def toCsv(self, csv_file=None):
        csv_file = csv_file if csv_file else self.csvFileName
        self.writeCsvHeaders(csv_file)
        for msg in self.msgs:
            self.writeCsvMessage(msg)
        self.closeCsvFile()
