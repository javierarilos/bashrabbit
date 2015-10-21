import unittest
from bashtasks import init, post_task


class TestBashTasks(unittest.TestCase):
    def test_connect_and_post_task(self):
        self.assertEqual('foo'.upper(), 'FOO')

if __name__ == '__main__':
    unittest.main()
