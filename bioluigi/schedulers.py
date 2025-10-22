import json
import logging
import subprocess
from abc import abstractmethod, ABC
from datetime import timedelta
from subprocess import Popen
from typing import Optional

import luigi
from luigi.contrib.external_program import ExternalProgramRunError, ExternalProgramRunContext

from .config import bioluigi

logger = logging.getLogger(__name__)

cfg = bioluigi()

class ScheduledTask(ABC, luigi.Task):
    """Interface for Luigi tasks that can be scheduled."""
    walltime: timedelta

    cpus: int
    memory: float

    scheduler_partition: Optional[str]
    scheduler_extra_args: list[str]

    capture_output: bool

    working_directory: Optional[str]

    @abstractmethod
    def program_environment(self) -> dict[str, str]:
        pass

    @abstractmethod
    def program_args(self) -> list[str]:
        pass

def _run_command(args, env, cwd, capture_output):
    kwargs = {'env': env, 'universal_newlines': True}
    if capture_output:
        kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE
    if cwd:
        kwargs['cwd'] = cwd
    logger.info('Running command %s%s', ' '.join(args), 'in ' + cwd if cwd else '')
    proc = Popen(args, **kwargs)
    with ExternalProgramRunContext(proc):
        if capture_output:
            stderr, stdout = proc.communicate()
            if stdout:
                logger.info('Program stdout:\n{}'.format(stdout))
            if stderr:
                logger.info('Program stderr:\n{}'.format(stderr))
        else:
            proc.wait()
    if proc.returncode != 0:
        raise ExternalProgramRunError('Program exited with non-zero return code.', tuple(args), env, stdout, stderr)

class Scheduler(ABC):
    @abstractmethod
    def get_resources_for_task(self, task: ScheduledTask) -> dict[str, int]:
        pass

    @abstractmethod
    def run_task(self, task: ScheduledTask):
        pass

_schedulers: dict[str, Scheduler] = {}

def register_scheduler(blurb):
    """
    :param blurb: Short name by with the scheduler is referred to
    or if it should be done through Luigi's resource management system.
    """

    global _schedulers

    def wrapper(cls):
        _schedulers[blurb] = cls()
        return cls

    return wrapper

def get_available_schedulers():
    return _schedulers.keys()

def get_scheduler(blurb):
    try:
        return _schedulers[blurb]
    except KeyError:
        raise ValueError('Unsupported scheduler {}'.format(blurb))

class SlurmSchedulerConfig(luigi.Config):
    @classmethod
    def get_task_family(cls):
        return 'bioluigi.schedulers.slurm'

    task_family = 'slurm'
    srun_bin: str = luigi.Parameter(default='srun')
    partition: Optional[str] = luigi.OptionalParameter(default=None)
    extra_args: list[str] = luigi.ListParameter(default=[])

slurm_cfg = SlurmSchedulerConfig()

@register_scheduler('slurm')
class SlurmScheduler(Scheduler):
    """
    Scheduler based on Slurm https://slurm.schedmd.com/
    """

    def get_resources_for_task(self, task: ScheduledTask):
        return {'slurm_jobs': 1, 'slurm_cpus': task.cpus}

    def run_task(self, task: ScheduledTask):
        secs = int(task.walltime.total_seconds())
        args = [slurm_cfg.srun_bin]
        args.extend([
            '--verbose',
            '--job-name', task.get_task_family(),
            '--comment', json.dumps({'task_id': task.task_id, 'priority': task.priority}),
            '--time',
            '{}-{:02d}:{:02d}:{:02d}'.format(secs // 86400, (secs % 86400) // 3600, (secs % 3600) // 60, secs % 60),
            '--mem', '{}G'.format(int(task.memory)),
            '--cpus-per-task', str(task.cpus)])
        if task.scheduler_partition:
            args.extend(['--partition', task.scheduler_partition])
        elif slurm_cfg.partition:
            args.extend(['--partition', slurm_cfg.partition])
        if task.working_directory:
            args.extend(['--chdir', task.working_directory])
        # FIXME: task.priority is not reliable and does not reflect what the
        # scheduler
        # TODO: srun_args.extend([--priority', str(max(0, cfg.scheduler_priority))])
        args.extend(map(str, slurm_cfg.extra_args))
        args.extend(map(str, task.scheduler_extra_args))
        args.extend(map(str, task.program_args()))
        _run_command(args, env=(task.program_environment()), cwd=task.working_directory,
                     capture_output=task.capture_output)

@register_scheduler('local')
class LocalScheduler(Scheduler):
    """A scheduler that uses a local subprocess"""

    def get_resources_for_task(self, task: ScheduledTask):
        return {'cpus': task.cpus, 'memory': task.memory}

    def run_task(self, task: ScheduledTask):
        _run_command(list(map(str, task.program_args())), env=task.program_environment(), cwd=task.working_directory,
                     capture_output=task.capture_output)

class SshSchedulerConfig(luigi.Config):
    @classmethod
    def get_task_family(cls):
        return 'bioluigi.schedulers.ssh'

    ssh_bin: str = luigi.Parameter(default='ssh')
    remote: Optional[str] = luigi.OptionalParameter(default=None)
    port: Optional[int] = luigi.OptionalIntParameter(default=None)
    user: Optional[str] = luigi.OptionalParameter(default=None)
    identity_file: Optional[str] = luigi.OptionalParameter(default=None)
    extra_args: list[str] = luigi.ListParameter(default=[])

ssh_cfg = SshSchedulerConfig()

@register_scheduler('ssh')
class SshScheduler(Scheduler):
    """A scheduler that uses SSH to run a task remotely"""

    def get_resources_for_task(self, task: ScheduledTask):
        return {'ssh_cpus': task.cpus, 'ssh_memory': task.memory}

    def run_task(self, task: ScheduledTask):
        if not ssh_cfg.remote:
            raise ValueError('No SSH remote is configured.')
        args = [ssh_cfg.ssh_bin]
        if ssh_cfg.port:
            args.extend(['-p', str(ssh_cfg.port)])
        if ssh_cfg.user:
            args.extend(['-u', ssh_cfg.user])
        if ssh_cfg.identity_file:
            args.extend(['-i', ssh_cfg.identity_file])
        args.extend(task.scheduler_extra_args)
        args.append(ssh_cfg.remote)
        args.extend(map(str, ssh_cfg.extra_args))
        args.extend(map(str, task.program_args()))
        _run_command(args, env=task.program_environment(), cwd=task.working_directory,
                     capture_output=task.capture_output)
