# bashtasks
Execute bash commands remotely, using a competing consumer model.

# simple example
```python
import bashtasks
x = bashtasks.init(host='127.0.0.1', usr='guest', pas='guest')
x.post_task('ls -la')  # when done, result will be in bashtasks:pool:responses queue
```

## TODO list
* Define public API
* post_task
* reply_to: reply to sender // reply to responses pool
* Test it
* implement reconnect.
