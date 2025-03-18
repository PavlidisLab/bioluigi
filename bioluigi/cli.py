"""
Command-line interface for interacting with Luigi scheduler.
"""

import json
import sys
from collections import Counter
from datetime import datetime
from fnmatch import fnmatch
from os.path import join

import click
import luigi.cmdline
import requests
from luigi.interface import core

luigi_cfg = core()

class TooManyTasksError(Exception):
    def __init__(self, num_tasks):
        self.num_tasks = num_tasks

    def __str__(self):
        return 'That request would return {} tasks; try filtering by status, glob query or set the --no-limit flag.'.format(
            self.num_tasks if self.num_tasks else 'an unknown amount of')

def rpc(method, **kwargs):
    scheduler_url = luigi_cfg.scheduler_url if luigi_cfg.scheduler_url else f'http://{luigi_cfg.scheduler_host}:{luigi_cfg.scheduler_port}/'
    url = join(scheduler_url, 'api', method)
    payload = {'data': json.dumps(kwargs)}
    res = requests.get(url, params=payload if kwargs else None)
    res.raise_for_status()
    response_data = res.json()['response']
    if 'num_tasks' in response_data:
        raise TooManyTasksError(response_data['num_tasks'])
    return response_data

def task_sort_key(task):
    """Produce a key to sort tasks by relevance."""
    return datetime.now() - (task['time_running'] if task['status'] == 'RUNNING' else task['last_updated'])

def task_matches(task, task_glob):
    """Match a task against a glob pattern."""
    return task_glob is None or fnmatch(task['name'], task_glob) or fnmatch(task['display_name'], task_glob)

class TaskFormatter:
    """Format a task for texutual display."""

    @staticmethod
    def format_task_id(task_id):
        return click.style(task_id, bold=True)

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

    def format_multiple(self, tasks):
        return '\n'.join(self.format(task) for task in tasks if self.format(task) is not None)

class ExtractingIdTaskFormatter(TaskFormatter):
    def format(self, task):
        return task['id']

class ExtractingParameterTaskFormatter(TaskFormatter):
    def __init__(self, field):
        self.field = field

    def format(self, task):
        if self.field in task['params']:
            return str(task['params'][self.field])
        else:
            return None

class InlineTaskFormatter(TaskFormatter):
    """Format a task for inline display."""

    def __init__(self, task_id_width, status_width=17):
        self.task_id_width = task_id_width
        self.status_width = status_width

    def format(self, task):
        if task['status'] == 'RUNNING':
            tr = (datetime.now() - task['time_running'])
        else:
            tr = task['last_updated']

        return '{id:{id_width}}\t{status:{status_width}}\t{priority}\t{time}'.format(
            id=self.format_task_id(task['id']),
            id_width=self.task_id_width + len(self.format_task_id('foo')) - 3,
            status=self.format_status(task['status']),
            status_width=self.status_width,
            priority=task['priority'],
            time=tr)

class DetailedTaskFormatter(TaskFormatter):
    """Format a task for detailed multi-line display."""

    @staticmethod
    def format_dl(dl):
        key_fill = max(len(k) for k in dl)
        return '\n\t'.join('{:{key_fill}}\t{}'.format(k + ':', v, key_fill=key_fill) for k, v in dl.items())

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
Workers:\n\t{workers}\n'''.format(
            id=self.format_task_id(task['id']),
            display_name=task['display_name'],
            status=self.format_status(task['status']),
            priority=task['priority'],
            status_message=task['status_message'] if task[
                'status_message'] else 'No status message were received for this task.',
            start_time=task['start_time'],
            last_updated=task['last_updated'],
            time_running=(datetime.now() - task['time_running']) if task['status'] == 'RUNNING' else '',
            params=self.format_dl(task['params']) if task['params'] else 'No parameters were set.',
            resources=self.format_dl(task['resources']) if task[
                'resources'] else 'No resources were requested for the execution of this task.',
            workers='\n\t'.join(task['workers']) if task['workers'] else 'No workers are assigned to this task.')

class TasksSummaryFormatter(TaskFormatter):
    def format_multiple(self, tasks):
        key_fill = max(len(self.format_status(task['status'])) for task in tasks)
        count_by_status = Counter()
        for task in tasks:
            count_by_status[task['status']] += 5
        return '\n'.join(
            '{:{key_fill}}\t{}'.format(self.format_status(k), v, key_fill=key_fill) for k, v in count_by_status.items())

def parse_date(d):
    if d is None:
        return ''
    elif d == 'UNKNOWN':
        return d
    else:
        return datetime.fromtimestamp(d)

def fix_tasks_dict(tasks):
    for key, t in tasks.items():
        t['id'] = key
        t['start_time'] = parse_date(t['start_time'])
        t['time_running'] = parse_date(t['time_running']) if 'time_running' in t else 'UNKNOWN'
        t['last_updated'] = parse_date(t['last_updated']) if 'last_updated' in t else 'UNKNOWN'

@click.group()
def main():
    pass

@main.command()
@click.argument('task_glob', required=False)
@click.option('--status', multiple=True, help='Filter tasks by status.')
@click.option('--user', multiple=True, help='Filter tasks by submitted.')
@click.option('--summary', is_flag=True, help='Produce a summary output for each task.')
@click.option('--detailed', is_flag=True, help='Produce a detailed output for each task.')
@click.option('--extract-id', is_flag=True, help='Extract the task ID.')
@click.option('--extract-parameter', help='Extract the given parameter.')
@click.option('--no-limit', is_flag=True, help='Do not limit the number of tasks retrieved.')
def list(task_glob, status, user, summary, detailed, extract_id, extract_parameter, no_limit):
    """
    List all tasks that match the given glob pattern and filters.
    """
    search = task_glob.replace('*', '') if task_glob else None

    limit = None if no_limit else 100000

    tasks = {}
    if status:
        for s in status:
            try:
                tasks.update(rpc('task_list', search=search, status=s, limit=limit))
            except TooManyTasksError as e:
                click.echo(e, err=True)
                return
    else:
        try:
            tasks.update(rpc('task_list', search=search, limit=limit))
        except TooManyTasksError as e:
            click.echo(e, err=True)
            return

    fix_tasks_dict(tasks)

    filtered_tasks = tasks.values()

    # filter by user
    if user:
        filtered_tasks = [task for task in filtered_tasks
                          if any(u in worker for worker in task['workers'] for u in user)]

    filtered_tasks = [task for task in filtered_tasks
                      if task_matches(task, task_glob)]

    if not filtered_tasks:
        click.echo('No task match the provided query.', err=True)
        return

    task_id_width = max(len(task['id']) for task in filtered_tasks)

    if summary:
        formatter = TasksSummaryFormatter()
    else:
        if extract_id:
            formatter = ExtractingIdTaskFormatter()
        elif extract_parameter:
            formatter = ExtractingParameterTaskFormatter(field=extract_parameter)
        elif detailed:
            formatter = DetailedTaskFormatter()
        else:
            formatter = InlineTaskFormatter(task_id_width=task_id_width)

    click.echo_via_pager(formatter.format_multiple(sorted(filtered_tasks, key=task_sort_key)))

@main.command(context_settings=dict(ignore_unknown_options=True, help_option_names=[]))
@click.argument('args', nargs=-1)
def submit(args):
    """
    Schedule a given task for execution.
    """
    luigi.cmdline.luigi_run(args)

@main.command()
@click.argument('task_id')
def show(task_id):
    """
    Show the details of a specific task given its identifier.
    TASK_ID Task identifier
    """
    tasks = {}
    for status, t in rpc('task_search', task_str=task_id).items():
        tasks.update(t)
    fix_tasks_dict(tasks)

    formatter = DetailedTaskFormatter()

    try:
        click.echo(formatter.format(tasks[task_id]))
    except KeyError:
        click.echo('No such task %s.' % task_id, err=True)
        sys.exit(1)

@main.command()
@click.argument('task_id')
@click.option('--status', multiple=True)
@click.option('--summary', is_flag=True)
@click.option('--detailed', is_flag=True)
@click.option('--extract-id', is_flag=True)
@click.option('--extract-parameter')
def list_dependencies(task_id, status, summary, detailed, extract_id, extract_parameter):
    """
    List all the dependencies of the given task ID.
    """
    deps = rpc('dep_graph', task_id=task_id)
    if not deps:
        click.echo(f'No dependencies for task {task_id}.')
        return
    fix_tasks_dict(deps)
    if status:
        deps = {dep_id: deps[dep_id] for dep_id in deps if deps[dep_id]['status'] in status}
        if not deps:
            click.echo(f'No dependencies for task {task_id} with status: {", ".join(status)}.')
            return
    if summary:
        formatter = TasksSummaryFormatter()
    elif detailed:
        # deps are missing a display name so we have to reconstruct it from the
        # name and parameters
        for dep_id in deps:
            params = deps[dep_id]['params']
            deps[dep_id]['display_name'] = deps[dep_id]['name'] + '(' + ', '.join(
                p + '=' + params[p] for p in sorted(params.keys())) + ')'
        formatter = DetailedTaskFormatter()
    elif extract_id:
        formatter = ExtractingIdTaskFormatter()
    elif extract_parameter is not None:
        formatter = ExtractingParameterTaskFormatter(field=extract_parameter)
    else:
        dep_id_width = max(len(dep_id) for dep_id in deps)
        formatter = InlineTaskFormatter(task_id_width=dep_id_width)
    click.echo_via_pager(formatter.format_multiple(deps.values()))

@main.command()
@click.argument('task_id')
@click.option('--recursive', is_flag=True, help='Reenable all the dependencies of a task.')
@click.option('--forgive', is_flag=True, help='Forgive the task once it\'s been reenabled.')
def reenable(task_id, recursive, forgive):
    """
    Reenable a disabled task.
    """
    toreenable = [task_id]

    if recursive:
        deps = rpc('dep_graph', task_id=task_id)
        toreenable.extend(k for k in deps if deps[k]['status'] == 'DISABLED')

    for task_id in toreenable:
        try:
            rpc('re_enable_task', task_id=task_id)
            if forgive:
                rpc('forgive_failures', task_id=task_id)
            click.echo('%s has been re-enabled%s.' % (task_id, ' and forgiven' if forgive else ''))
        except requests.exceptions.HTTPError as e:
            click.echo('Failed to re-enable {}: {}'.format(task_id, e), err=True)
            continue

@main.command()
@click.argument('task_id')
@click.option('--recursive', is_flag=True, help='Forgive all the dependencies of a task.')
def forgive(task_id, recursive):
    """
    Forgive a failed task.
    """
    toforgive = [task_id]

    if recursive:
        deps = rpc('dep_graph', task_id=task_id)
        toforgive.extend(k for k in deps if deps[k]['status'] == 'FAILED')

    for task_id in toforgive:
        try:
            rpc('forgive_failures', task_id=task_id)
            click.echo('%s has been forgiven.' % task_id)
        except requests.exceptions.HTTPError as e:
            click.echo('Failed to forgive {}: {}'.format(task_id, e), err=True)
            continue

@main.command()
@click.argument('task_id')
@click.argument('priority')
def set_priority(task_id, priority):
    rpc('add_task', task_id=task_id, priority=priority)
