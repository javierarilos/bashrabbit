import unittest
from bashtasks.message import get_request, from_str
from bashtasks.constants import Destination

command = ['ps', '-axf']
max_retries = 5


class TestBashTasksMessage(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_msg_factory_simple(self):
        msg = get_request(command, reply_to=Destination.responses_pool, max_retries=max_retries)

        self.assertEqual(msg['command'], command)
        self.assertEqual(msg['max_retries'], max_retries)
        self.assertTrue('correlation_id' in msg)
        self.assertTrue('request_ts' in msg)

    def test_to_json(self):
        msg = get_request(command, reply_to=Destination.responses_pool, max_retries=max_retries)

        msg_str = msg.to_json()

        self.assertIsInstance(msg_str, str)

    def test_serialize_deserialize(self):
        msg = get_request(command, reply_to=Destination.responses_pool, max_retries=max_retries)

        msg_str = msg.to_json()

        msg_copy = from_str(msg_str)

        self.assertEqual(msg, msg_copy)
