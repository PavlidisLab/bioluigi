"""
Command-line interface for interacting with Luigi scheduler.
"""

import json
import requests
import datetime
import click
from fnmatch import fnmatch
import sys
from os.path import join
from collections import Counter

def rpc(scheduler_url, method, **kwargs):
    url = join(scheduler_url, 'api', method)
    payload = {'data': json.dumps(kwargs)}
    res = requests.get(url, params=payload if kwargs else None)
    res.raise_for_status()
    return res.json()['response']

def task_sort_key(task):
    """Produce a key to sort tasks by relevance."""
    return datetime.datetime.now() - (task['time_running'] if task['status'] == 'RUNNING' else task['last_updated'])

def task_matches(task, task_glob):
    """Match a task against a glob pattern."""
    return task_glob is None or fnmatch(task['name'], task_glob) or fnmatch(task['display_name'], task_glob)

class TaskFormatter(object):
    """Format a task for texutual display."""
    @staticmethod
    def format_status(status):
        tg = {'DONE': 'green',
              'PENDING': 'yellow',
              'RUNNING': 'blue',
              'FAILED': 'red',
              'DISABLED': 'white',
              'UNKNOWN': 'white'}
        return click.style(status, fg=tg[status]) if status in tg else status
    def format(self, task):
        raise NotImplementedError

class InlineTaskFormatter(TaskFormatter):
    """Format a task for inline display."""
    def __init__(self, task_id_width, status_width=17):
        self.task_id_width = task_id_width
        self.status_width = status_width

    def format(self, task):
        if task['status'] == 'RUNNING':
            tr = (datetime.datetime.now() - task['time_running'])
        else:
            tr = task['last_updated']

        return '{id:{id_width}}\t{status:{status_width}}\t{time}\n'.format(
                id=click.style(task['id'], bold=True),
                id_width=self.task_id_width,
                status=self.format_status(task['status']),
                status_width=self.status_width,
                time=tr)

class DetailedTaskFormatter(TaskFormatter):
    """Format a task for detailed multi-line display."""
    @staticmethod
    def format_dl(dl):
        key_fill = max(len(click.style(k)) for k in dl)
        return '\n\t'.join('{:{key_fill}}\t{}'.format(click.style(k, bold=True) + ':', v, key_fill=key_fill) for k, v in dl.items())

    def format(self, task):
        return '''{id}
Status:      \t{status}
Priority:    \t{priority}
Name:        \t{display_name}
Start time:  \t{start_time}
Last updated:\t{last_updated}
Time running:\t{time_running}
Status message:\n\t{status_message}
Parameters:\n\t{params}
Resources:\n\t{resources}
Workers:\n\t{workers}\n\n'''.format(
        id=click.style(task['id'], bold=True),
        display_name=task['display_name'],
        status=self.format_status(task['status']),
        priority=task['priority'],
        status_message=task['status_message'] if task['status_message'] else 'No status message were received for this task.',
        start_time=task['start_time'],
        last_updated=task['last_updated'],
        time_running=(datetime.datetime.now() - task['time_running']) if task['status'] == 'RUNNING' else '',
        params=self.format_dl(task['params']) if task['params'] else 'No parameters were set.',
        resources=self.format_dl(task['resources']) if task['resources'] else 'No resources were requested for the execution of this task.',
        workers='\n\t'.join(task['workers']) if task['workers'] else 'No workers are assigned to this task.')

class TasksSummaryFormatter(object):
    def format(self, tasks):
        count_by_status = Counter()
        for task in tasks:
            count_by_status[task['status']] += 1
        return '\n'.join('{}\t{}'.format(TaskFormatter.format_status(k), v) for k, v in count_by_status.items())

def fix_tasks_dict(tasks):
    for key, t in tasks.items():
        t['id'] = key
        t['start_time'] = t['start_time'] and datetime.datetime.fromtimestamp(t['start_time'])
        t['time_running'] = t['time_running'] and datetime.datetime.fromtimestamp(t['time_running'])
        t['last_updated'] = t['last_updated'] and datetime.datetime.fromtimestamp(t['last_updated'])

@click.group()
@click.option('--scheduler-url', default='http://localhost:8082/')
@click.pass_context
def main(ctx, scheduler_url):
    ctx.obj = {}
    ctx.obj['SCHEDULER_URL'] = scheduler_url
    # TODO: defer this, not all commands need the full task list
    tasks = rpc(scheduler_url, 'task_list')
    fix_tasks_dict(tasks)
    ctx.obj['TASKS'] = tasks

@main.command()
@click.argument('task_glob', required=False)
@click.option('--status', multiple=True)
@click.option('--user', multiple=True)
@click.option('--summary', is_flag=True)
@click.option('--detailed', is_flag=True)
@click.pass_context
def list(ctx, task_glob, status, user, summary, detailed):
    """
    List all tasks that match the given pattern and filters.
    """
    tasks = ctx.obj['TASKS']

    filtered_tasks = tasks.values()

    # filter by user
    if user:
        filtered_tasks = [task for task in filtered_tasks
                          if any(u in worker for worker in task['workers'] for u in user)]

    # filter by status
    if status:
        filtered_tasks = [task for task in filtered_tasks if task['status'] in status]

    filtered_tasks = [task for task in filtered_tasks
                      if task_matches(task, task_glob)]

    if not filtered_tasks:
        click.echo('No task match the provided query.')
        return

    task_id_width = max(len(click.style(task['id'], bold=True)) for task in filtered_tasks)

    if summary:
        formatter = TasksSummaryFormatter()
        click.echo(formatter.format(filtered_tasks))
    else:
        if detailed:
            formatter = DetailedTaskFormatter()
        else:
            formatter = InlineTaskFormatter(task_id_width=task_id_width)

        click.echo_via_pager(formatter.format(t) for t in sorted(filtered_tasks, key=task_sort_key))

@main.command()
def submit(*args):
    """
    Schedule a given task for execution.
    """
    luigi.cmdline.luigi_run(args)

@main.command()
@click.argument('task_id')
@click.pass_context
def show(ctx, task_id):
    """
    Show the details of a specific task given its identifier.
    TASK_ID Task identifier
    """
    scheduler_url = ctx.obj['SCHEDULER_URL']
    tasks = ctx.obj['TASKS']

    formatter = DetailedTaskFormatter()

    try:
        click.echo(formatter.format(tasks[task_id]))
    except KeyError:
        click.echo('No such task %s.' % task_id)
        sys.exit(1)

@main.command()
@click.argument('task_id')
@click.option('--recursive', is_flag=True)
@click.pass_context
def reenable(ctx, task_id, recursive):
    """
    Reenable a disabled task.
    """
    scheduler_url = ctx.obj['SCHEDULER_URL']

    toreenable = [task_id]

    if recursive:
        deps = rpc(scheduler_url, 'dep_graph', task_id=task_id)
        toreenable.extend(deps.keys())

    for task_id in toreenable:
        try:
            rpc(scheduler_url, 're_enable_task', task_id=task_id)
            click.echo('%s has been re-enabled.' % task_id)
        except requests.exceptions.HTTPError as e:
            click.echo('Failed to re-enable {}: {}'.format(task_id, e))
            continue

@main.command()
@click.argument('task_id')
@click.option('--recursive', is_flag=True)
@click.pass_context
def forgive(ctx, task_id, recursive):
    """
    Forgive a failed task.
    """
    scheduler_url = ctx.obj['SCHEDULER_URL']

    toforgive = []

    if recursive:
        deps = rpc(scheduler_url, 'dep_graph', task_id=task_id)
        toforgive.extend(deps.keys())

    for task_id in toforgive:
        try:
            rpc(scheduler_url, 'forgive_failures', task_id=task_id)
            click.echo('%s has been forgiven.' % task_id)
        except requests.exceptions.HTTPError as e:
            click.echo('Failed to forgive {}: {}'.format(task_id, e))
            continue
