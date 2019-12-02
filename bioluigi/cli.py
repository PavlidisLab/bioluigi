"""
Command-line interface for interacting with Luigi scheduler.
"""

import requests
import datetime
import click
from fnmatch import fnmatch
import sys
from os.path import join

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
    def __init__(self, task_id_width, status_width=17):
        self.task_id_width = task_id_width

    @staticmethod
    def format_dl(dl):
        key_fill = max(len(click.style(k)) for k in dl)
        return '\n\t'.join('{:{key_fill}}\t{}'.format(click.style(k, bold=True) + ':', v, key_fill=key_fill) for k, v in dl.items())

    def format(self, task):
        return '''{id:{id_width}}\t{status}
Name:        \t{display_name}
Status:      \t{status_message}
Start time:  \t{start_time}
Time running:\t{time_running}
Parameters:\n\t{params}
Resources:\n\t{resources}
Workers:\n\t{workers}\n\n'''.format(id=click.style(task['id'], bold=True),
        id_width=self.task_id_width,
        display_name=task['display_name'],
        status=self.format_status(task['status']),
        status_message=task['status_message'] if task['status_message'] else '',
        start_time=task['start_time'],
        time_running=(datetime.datetime.now() - task['time_running']) if task['status'] == 'RUNNING' else '',
        params=self.format_dl(task['params']) if task['params'] else 'No parameters were set.',
        resources=self.format_dl(task['resources']) if task['resources'] else 'No resources were requested.',
        workers='\n\t'.join(task['workers']) if task['workers'] else 'No workers were assigned.')

@click.group()
@click.option('--scheduler-url', default='http://localhost:8082/')
def main(scheduler_url):
    global tasks
    res = requests.get(join(scheduler_url, 'api/task_list'))
    res.raise_for_status()
    tasks = res.json()['response']
    for key, t in tasks.items():
        t['id'] = key
        t['start_time'] = t['start_time'] and datetime.datetime.fromtimestamp(t['start_time'])
        t['time_running'] = t['time_running'] and datetime.datetime.fromtimestamp(t['time_running'])
        t['last_updated'] = t['last_updated'] and datetime.datetime.fromtimestamp(t['last_updated'])

@main.command()
@click.argument('task_glob', required=False)
@click.option('--status', multiple=True)
@click.option('--user', multiple=True)
@click.option('--detailed', is_flag=True)
def list(task_glob, status, user, detailed):
    """
    List all tasks that match the given pattern and filters.
    """
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

    if detailed:
        formatter = DetailedTaskFormatter(task_id_width=task_id_width)
    else:
        formatter = InlineTaskFormatter(task_id_width=task_id_width)

    click.echo_via_pager(formatter.format(t) for t in sorted(filtered_tasks, key=task_sort_key))

@main.command()
@click.argument('task_id')
@click.option('--deps', is_flag=True)
@click.option('--graph', is_flag=True)
def show(task_id, deps, graph):
    """
    Show the details of a specific task given its identifier.
    TASK_ID Task identifier
    """
    formatter = DetailedTaskFormatter(task_id_width=len(task_id))
    try:
        click.echo(formatter.format(tasks[task_id]))
    except KeyError:
        click.echo('No such task %s.' % task_id)
        sys.exit(1)
