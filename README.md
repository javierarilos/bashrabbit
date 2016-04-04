# bashtasks
Execute bash commands remotely, using a competing consumer model.

# simple example
```python
import bashtasks
x = bashtasks.init(host='127.0.0.1', usr='guest', pas='guest')
x.post_task('ls -la')  # when done, result will be in bashtasks:pool:responses queue
```

## TODO list
* implement reconnect.
* stdout, stderr response policy: all, only_stdin, only_stdout, on_error
