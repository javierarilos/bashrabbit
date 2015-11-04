import unittest
from bashtasks import TaskStatistics


class TestBashTasks(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_init_TaskStatistics(self):
        stats = TaskStatistics()

        self.assertEqual(stats.msgs_nr, 0)
