import unittest
import bashtasks


class TestBashTasks(unittest.TestCase):

    def test_init_returns_bashtasks(self):
        bashtask = bashtasks.init(channel='dummy')
        isBashTask = hasattr(bashtask, 'post_task')
        self.assertTrue(isBashTask)

if __name__ == '__main__':
    unittest.main()
