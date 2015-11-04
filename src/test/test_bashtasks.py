import unittest
import time
from datetime import datetime
import multiprocessing

import bashtasks as bashtasks_mod
import bashtasks.rabbit_util as rabbit_util
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL
from test.pika_assertions import assertMessageInQueue
import executor

unavailable_rabbit = not rabbit_util.is_rabbit_available()


class FakeChannel:
    def close(*args, **kwargs):
        pass


class TestBashTasks(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        bashtasks_mod.reset()

    def test_init_returns_bashtasks(self):
        bashtask = bashtasks_mod.init(channel=FakeChannel())
        isBashTask = hasattr(bashtask, 'post_task')
        self.assertTrue(isBashTask)


@unittest.skipIf(unavailable_rabbit, "SKIP integration Tests: rabbitmq NOT available")
class IntegTestPostTask(unittest.TestCase):
    def setUp(self):
        rabbit_util.purge()

    def tearDown(self):
        rabbit_util.purge()
        bashtasks_mod.reset()

    def test_post_task_sends_message(self):
        bashtasks = bashtasks_mod.init()
        bashtasks.post_task(['ls', '-la'])

        body = assertMessageInQueue(TASK_REQUESTS_POOL)

    def test_post_task_creates_correct_task_msg(self):
        ls_task = ['ls', '-la']
        bashtasks = bashtasks_mod.init()
        msg = bashtasks.post_task(ls_task)

        self.assertEqual(msg['command'], ls_task)
        self.assertEqual(msg['reply_to'], TASK_RESPONSES_POOL)
        self.assertTrue('correlation_id' in msg)
        self.assertTrue('request_ts' in msg)


def start_executor_process():
    p = multiprocessing.Process(target=executor.start_executor)
    p.start()
    time.sleep(0.1)
    return p


def kill_executor_process(p):
    p.terminate()


@unittest.skipIf(unavailable_rabbit, "SKIP integration Tests: rabbitmq NOT available")
class IntegTestExecuteTask(unittest.TestCase):
    def setUp(self):
        rabbit_util.purge()

    def tearDown(self):
        rabbit_util.purge()
        bashtasks_mod.reset()

    def test_execute_task_raises_on_timeout(self):
        ls_task = ['ls', '-la']
        bashtasks = bashtasks_mod.init()
        with self.assertRaises(Exception):
            response_msg = bashtasks.execute_task(ls_task, timeout=0.3)

    def test_execute_task_returns_correct_response_msg(self):
        try:
            p = start_executor_process()

            bashtasks = bashtasks_mod.init()
            ls_task = ['ls', '-la']
            response_msg = bashtasks.execute_task(ls_task)

            self.assertTrue('returncode' in response_msg)
            self.assertTrue('request_ts' in response_msg)
            self.assertEqual(response_msg['command'], ls_task)
            self.assertEqual(response_msg['reply_to'], TASK_RESPONSES_POOL)
            self.assertTrue('correlation_id' in response_msg)
        finally:
            kill_executor_process(p)
            time.sleep(0.2)


if __name__ == '__main__':
    unittest.main()
