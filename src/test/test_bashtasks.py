import unittest
import time
import os
from datetime import datetime
import multiprocessing
import threading
from time import sleep
import json

import bashtasks as bashtasks_mod
import bashtasks.rabbit_util as rabbit_util
from bashtasks.constants import TASK_REQUESTS_POOL, TASK_RESPONSES_POOL
from bashtasks.pika_assertions import assertMessageInQueue
import bashtasks.executor as executor

rabbit_host = os.getenv('RABBIT_HOST', '127.0.0.1')
rabbit_user = os.getenv('RABBIT_USER', 'guest')
rabbit_pass = os.getenv('RABBIT_PASS', 'guest')

unavailable_rabbit = not rabbit_util.is_rabbit_available(host=rabbit_host,
                                                         usr=rabbit_user,
                                                         pas=rabbit_pass)


class FakeConnection:
    is_open = False

    def close(*args, **kwargs):
        pass


class FakeChannel:
    _impl = FakeConnection()
    is_open = False

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
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)

    def tearDown(self):
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
        bashtasks_mod.reset()

    def test_post_task_sends_message(self):
        bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
        bashtasks.post_task(['ls', '-la'])

        body = assertMessageInQueue(TASK_REQUESTS_POOL, host=rabbit_host,
                                    usr=rabbit_user, pas=rabbit_pass)

    def test_post_task_creates_correct_task_msg(self):
        ls_task = ['ls', '-la']
        bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
        msg = bashtasks.post_task(ls_task)

        self.assertEqual(msg['command'], ls_task)
        self.assertEqual(msg['reply_to'], TASK_RESPONSES_POOL)
        self.assertTrue('correlation_id' in msg)
        self.assertTrue('request_ts' in msg)


def start_executor_process(tasks_nr=1):
    p = multiprocessing.Process(target=executor.start_executor,
                                kwargs=({'host': rabbit_host, 'usr': rabbit_user,
                                         'pas': rabbit_pass, 'tasks_nr': tasks_nr}))
    p.start()
    time.sleep(0.1)
    return p


def kill_executor_process(p):
    time.sleep(0.1)
    p.terminate()


@unittest.skipIf(unavailable_rabbit, "SKIP integration Tests: rabbitmq NOT available")
class IntegTestExecuteTask(unittest.TestCase):
    def setUp(self):
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)

    def tearDown(self):
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
        bashtasks_mod.reset()

    def test_execute_task_raises_on_timeout(self):
        ls_task = ['ls', '-la']
        bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
        with self.assertRaises(Exception):
            response_msg = bashtasks.execute_task(ls_task, timeout=0.3)

    def test_execute_task_returns_correct_response_msg(self):
        try:
            p = start_executor_process()

            bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
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


@unittest.skipIf(unavailable_rabbit, "SKIP integration Tests: rabbitmq NOT available")
class IntegTestTaskResponseSubscriber(unittest.TestCase):
    def setUp(self):
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)

    def tearDown(self):
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
        bashtasks_mod.reset()

    def test_subscribe(self):
        ls_task = ['ls', '-la']
        response_msg = {}

        def start_subscriber():
            def on_response_received(msg):
                body = json.loads(msg.body.decode('utf-8'))
                response_msg.update(body)
                msg.ack()
            subscriber = bashtasks_mod.init_subscriber(host=rabbit_host,
                                                       usr=rabbit_user, pas=rabbit_pass)
            subscriber.subscribe(on_response_received)

        try:
            # subscribe to responses.
            # prepare executor, and send task.
            # subscribe to responses and check response arrives
            p = start_executor_process()

            subscriber_th = threading.Thread(target=start_subscriber,
                                             args=(),
                                             name='subscriber_th')
            subscriber_th.daemon = True
            subscriber_th.start()

            bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
            posted_msg = bashtasks.post_task(ls_task)
            start_time = time.time()
            # give rabbit and subscriber time to work
            while 'command' not in response_msg and time.time() - start_time < 10:
                sleep(0.1)
            self.assertTrue('command' in response_msg, 'expected response_msg to include command')
            self.assertEqual(ls_task, response_msg['command'])
        except Exception as e:
            print('Got exception in test:', repr(e))
            import traceback
            print(traceback.format_exc())
            print('response_msg::::::', repr(response_msg))

        finally:
            kill_executor_process(p)
            time.sleep(0.2)


@unittest.skipIf(unavailable_rabbit, "SKIP integration Tests: rabbitmq NOT available")
class IntegTestRetries(unittest.TestCase):
    def setUp(self):
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)

    def tearDown(self):
        rabbit_util.purge(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
        bashtasks_mod.reset()

    def test_execute_task_OK_returns_zero_retries(self):
        try:
            p = start_executor_process()
            max_retries = 5
            bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
            ls_task = ['ls', '-la']
            response_msg = bashtasks.execute_task(ls_task, max_retries=max_retries)

            expect_retries = 'response.retries should be {}'.format(0)
            self.assertEqual(response_msg['retries'], 0, expect_retries)

            expect_max_retries = 'response.max_retries should be {}'.format(max_retries)
            self.assertEqual(response_msg['max_retries'], max_retries, expect_max_retries)

            expect_returncode = 'response.returncode should be {}'.format(0)
            self.assertEqual(response_msg['returncode'], 0, expect_returncode)
        except Exception as e:
            print 'test_execute_task_OK_returns_zero_retries', e
            raise e

        finally:
            kill_executor_process(p)
            time.sleep(0.2)

    def test_execute_task_KO_returns_max_retries(self):
        try:
            max_retries = 2
            p = start_executor_process(tasks_nr=max_retries+1)
            bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
            ls_task = ['non-existent-command']
            response_msg = bashtasks.execute_task(ls_task, max_retries=max_retries)

            expect_retries = 'response.retries should be {}'.format(max_retries)
            self.assertEqual(response_msg['retries'], max_retries, expect_retries)

            expect_max_retries = 'response.max_retries should be {}'.format(max_retries)
            self.assertEqual(response_msg['max_retries'], max_retries, expect_max_retries)

            expect_returncode = 'response.returncode should be different to {}'.format(max_retries)
            self.assertNotEqual(response_msg['returncode'], 0, expect_returncode)
        except Exception as e:
            print 'test_execute_task_KO_returns_max_retries', e
            raise e
        finally:
            kill_executor_process(p)
            time.sleep(0.2)

    def test_execute_task_KO_with_nonretries(self):
        try:
            max_retries = 2
            expected_returncode = 2
            p = start_executor_process(tasks_nr=max_retries+1)
            bashtasks = bashtasks_mod.init(host=rabbit_host, usr=rabbit_user, pas=rabbit_pass)
            task = ['grep', 'bye', 'bye']
            response_msg = bashtasks.execute_task(task, max_retries=max_retries,
                                                  non_retriable=[expected_returncode])

            self.assertEqual(response_msg['retries'], 0)
            self.assertEqual(response_msg['max_retries'], max_retries)
            self.assertEqual(response_msg['returncode'], expected_returncode)
            self.assertEqual(response_msg['non_retriable'], [expected_returncode])
        except Exception as e:
            print 'test_execute_task_KO_with_nonretries', e
            raise e
        finally:
            kill_executor_process(p)
            time.sleep(0.2)


if __name__ == '__main__':
    unittest.main()
