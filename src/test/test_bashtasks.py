import unittest
import time
from datetime import datetime

import bashtasks as bashtasks_mod
import bashtasks.rabbit_util as rabbit_util
from bashtasks.constants import TASK_POOL
from pika_assertions import assertMessageInQueue

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
class IntegrationTesting(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        bashtasks_mod.reset()

    def test_post_task_sends_message(self):
        bashtasks = bashtasks_mod.init()
        bashtasks.post_task(['ls', '-la'])

        queue_name = TASK_POOL
        body = assertMessageInQueue(queue_name)


if __name__ == '__main__':
    unittest.main()
