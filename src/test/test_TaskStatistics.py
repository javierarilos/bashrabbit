import unittest
from bashtasks import TaskStatistics
import time

err_code = 7
WRKR_ONE = 'one1'
WRKR_TWO = 'two2'


def currtimemillis():
    return int(round(time.time() * 1000))


def get_msg(request_ts=None, pre_command_ts=None, post_command_ts=None, returncode=0,
            executor_name='exec1'):
    return {
            'correlation_id': time.time(),
            'request_ts': request_ts if request_ts else currtimemillis(),
            'pre_command_ts': pre_command_ts if pre_command_ts else currtimemillis() + 200,
            'post_command_ts': post_command_ts if post_command_ts else currtimemillis() + 1000,
            'executor_name': executor_name,
            'returncode': returncode
    }


def get_simple_experiment_stats():
    stats = TaskStatistics()
    now = 1446628389719
    # total durations: 1000, 1700, 1500
    # waiting: 100, 50, 150
    # executing: 900, 1650, 1350
    msg1 = get_msg(request_ts=now, pre_command_ts=now+100, post_command_ts=now+1000,
                   executor_name=WRKR_ONE)
    msg2 = get_msg(request_ts=now+100, pre_command_ts=now+150, post_command_ts=now+1800,
                   returncode=err_code, executor_name=WRKR_TWO)
    msg3 = get_msg(request_ts=now+200, pre_command_ts=now+350, post_command_ts=now+1700,
                   executor_name=WRKR_ONE)

    stats.trackMsg(msg1)
    stats.trackMsg(msg2)
    stats.trackMsg(msg3)

    return stats


class TestBashTasks(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_init_TaskStatistics(self):
        stats = TaskStatistics()

        self.assertEqual(len(stats.msgs), 0)

    def test_trackMsg(self):
        stats = TaskStatistics()
        msg = get_msg()

        stats.trackMsg(msg)

        self.assertTrue(msg in stats.msgs)

    def test_msgsNumber(self):
        stats = TaskStatistics()

        self.assertEqual(stats.msgsNumber(), 0)

        stats.trackMsg(get_msg())

        self.assertEqual(stats.msgsNumber(), 1)

        stats.trackMsg(get_msg())

        self.assertEqual(stats.msgsNumber(), 2)

    def test_avgTimeToExecuted(self):
        stats = get_simple_experiment_stats()

        # duration of messages / nr of msgs => 1000 + 1700 + 1500 // 3
        self.assertEqual(stats.avgTimeToExecuted(), 1400)

    def test_avgTimeWaiting(self):
        stats = get_simple_experiment_stats()

        # (100 + 50 + 150) // 3
        self.assertEqual(stats.avgTimeWaiting(), 100)

    def test_avgExecutionTime(self):
        stats = get_simple_experiment_stats()

        # (900 + 1650 + 1350) // 3
        self.assertEqual(stats.avgExecutionTime(), 1300)

    def test_maxTimetoExecuted(self):
        stats = get_simple_experiment_stats()

        self.assertEqual(stats.maxTimeToExecuted(), 1700)

    def test_allErrors(self):
        stats = get_simple_experiment_stats()

        all_errors = stats.allErrors()

        self.assertEqual(len(all_errors), 1)
        self.assertEqual(all_errors[0]['returncode'], err_code)

    def test_errorsNumber(self):
        stats = get_simple_experiment_stats()

        self.assertEqual(stats.errorsNumber(), 1)

    def test_okNumber(self):
        stats = get_simple_experiment_stats()

        self.assertEqual(stats.okNumber(), 2)

    def test_getWorkersCounter(self):
        stats = get_simple_experiment_stats()

        workers_counter = stats.getWorkersCounter()

        self.assertEqual(workers_counter[WRKR_ONE], 2)
        self.assertEqual(workers_counter[WRKR_TWO], 1)

    def test_getReturnCodesCounter(self):
        stats = get_simple_experiment_stats()

        return_codes_counter = stats.getReturnCodesCounter()

        self.assertEqual(return_codes_counter[0], 2)
        self.assertEqual(return_codes_counter[err_code], 1)
