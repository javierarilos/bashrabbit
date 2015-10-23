import unittest
import time
from datetime import datetime

import bashtasks as bashtasks_mod
import bashtasks.rabbit_util as rabbit_util
from bashtasks.constants import TASK_POOL

unavailable_rabbit = not rabbit_util.is_rabbit_available()


def assertMessageInQueue(queue_name, channel=None, timeout=3):
    if not channel:
        channel = rabbit_util.connect().channel()

    start_waiting = datetime.now()
    while True:
        method_frame, header_frame, body = channel.basic_get(queue_name)
        if body:
            channel.basic_ack(method_frame.delivery_tag)
            return body
        else:
            if (datetime.now() - start_waiting).total_seconds() > timeout:
                raise Exception('Timeout ({}secs) exceeded waiting for message in queue: "{}"'
                                .format(timeout, queue_name))
            time.sleep(0.01)


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
        timeout = 1
        queue_name = TASK_POOL
        channel = rabbit_util.connect().channel()
        body = assertMessageInQueue(queue_name, channel=channel, timeout=timeout)
        # print (')))))===>> received:', body)


if __name__ == '__main__':
    unittest.main()
