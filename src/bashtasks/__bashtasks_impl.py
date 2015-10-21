"""bashtasks implementation module
"""


def post_task():
    print '>>>>> posting task'


class BashTasks:
    pass


def init():
    print '>>>>> initializing bashtasks'
    o = BashTasks()
    o.post_task = post_task
    return o
