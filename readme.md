# Background job - Assignment

The goal is to build a Redis-backed package that allows creating background jobs, placing those jobs on multiple queues, 
and processing them later. Background jobs are an important part of any application stack.The main use case for them is 
when you have a (relatively) long running process which you want to move outside of the critical path of your 
application. For example when we call an API endpoint to retrain a model, we don't necessarily want the model training
process to be tied to the API call, the request might timeout, or the connection could get interrupted which would lead
to hard to diagnose problems. What we would like to probably happen instead, is that when we call that endpoint, we get
a job id back, and we can use that job id later on to poll the state of the job that is executing somewhere in the 
background. This pattern also allows us to utilize our hardware efficiently. We could have a background worker with a 
GPU that's good at running deep learning models, and we might not need our actual API server to run on that GPU 
powered machine.


The general pattern is something along these lines:

```python
import fonlaboristo
from flask import jsonify, Flask, request

app = Flask(__name__)
f = fonlaboristo.Queue(redis='....')

@f.task()
def my_task(param):
    ...

@app.route('/task/', methods=['POST'])
def run_task(param):
    job = my_task(**request.json)  # returns a job object containing the status of the job, instead of executing my_task directly
    return jsonify({
        'job': job.id,
        'status': job.status,
    })

@app.route('/task/<int:id>')
def get_task():
    job = f.get_job(id)
    return jsonify(job.status)
    
```

# Requirements:
- allow a job to be scheduled by putting it on Redis
- build a command line tool that starts a worker process that reads job off of redis and posts the results back


## Nice to haves:
- Handle failed jobs without losing them
- release dates
- cron jobs
- Priorities
- See what workers are doing
- See what workers have done
- See failed jobs
- Kill fat workers
- Kill stale workers
- Kill workers that are running too long
- Distributed workers (run them on multiple machines)
- Workers can watch multiple (or all) tags
