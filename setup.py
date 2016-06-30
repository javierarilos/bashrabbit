#!/usr/bin/env python

from setuptools import setup

setup(name='bashtasks',
      version='0.2',
      description='Execute bash commands remotely, using a competing consumer model.',
      author='Javier Arias',
      author_email='javier.arilos@gmail.com',
      url='https://github.com/javierarilos/bashtasks.git',
      packages=['bashtasks'],
      package_dir={'bashtasks': 'src/bashtasks'},
      install_requires=['pika'],
      scripts=['src/start_executor.py', 'src/execute_task.py', 'src/responses_recvr.py', 'queue_util']
      )
