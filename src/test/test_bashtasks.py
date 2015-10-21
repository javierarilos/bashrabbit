import unittest
from bashtasks import init
from bashtasks import post_task


class TestBashTasks(unittest.TestCase):
    def test_connect_and_post_task(self):
        init()
        post_task()
        self.assertEqual('foo'.upper(), 'FOO')

if __name__ == '__main__':
    unittest.main()
