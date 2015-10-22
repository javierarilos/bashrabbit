import unittest
import bashtasks
import bashtasks.rabbit_util as rabbit_util

unavailable_rabbit = not rabbit_util.is_rabbit_available()
channel_fake = 'fake'


class TestBashTasks(unittest.TestCase):
    def test_init_returns_bashtasks(self):
        bashtask = bashtasks.init(channel=channel_fake)
        isBashTask = hasattr(bashtask, 'post_task')
        self.assertTrue(isBashTask)


@unittest.skipIf(unavailable_rabbit, "SKIP integration Tests: rabbitmq NOT available")
class IntegrationTesting(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_post_task_sends_message(self):
        bashtask = bashtasks.init(channel='dummy')
        isBashTask = hasattr(bashtask, 'post_task')
        self.assertTrue(isBashTask)

if __name__ == '__main__':
    unittest.main()
