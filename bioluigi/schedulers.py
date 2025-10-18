import logging
from abc import abstractmethod, ABC, abstractclassmethod
from datetime import timedelta
from subprocess import Popen, PIPE
import json
from typing import Optional

import luigi

from luigi.contrib.external_program import ExternalProgramRunError, ExternalProgramRunContext

logger = logging.getLogger(__name__)

_schedulers = {}

class ScheduledTask(luigi.Task, ABC):
    """Interface for Luigi tasks that can be scheduled."""
    walltime: timedelta

    cpus: int
    memory: float

    scheduler_partition: Optional[str]
    scheduler_extra_args: list[str]

    capture_output: bool

    @abstractmethod
    def program_environment(self) -> dict[str, str]:
        pass

    @abstractmethod
    def program_args(self) -> list[str]:
        pass

def get_available_schedulers():
    return _schedulers.keys()

def get_scheduler(blurb):
    return _schedulers[blurb]

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

class Scheduler(ABC):
    @abstractmethod
    def run_task(self, task):
        pass

@register_scheduler('slurm')
class SlurmScheduler(Scheduler):
    """
    Scheduler based on Slurm https://slurm.schedmd.com/
    """

    def run_task(self, task: ScheduledTask):
        secs = int(task.walltime.total_seconds())
        srun_args = ['srun']
        srun_args.extend([
            '--verbose',
            '--job-name', task.get_task_family(),
            '--comment', json.dumps({'task_id': task.task_id, 'priority': task.priority}),
            '--time',
            '{}-{:02d}:{:02d}:{:02d}'.format(secs // 86400, (secs % 86400) // 3600, (secs % 3600) // 60, secs % 60),
            '--mem', '{}G'.format(int(task.memory)),
            '--cpus-per-task', str(task.cpus)])
        if task.scheduler_partition:
            srun_args.extend(['--partition', task.scheduler_partition])
        # FIXME: task.priority is not reliable and does not reflect what the
        # scheduler
        # TODO: srun_args.extend([--priority', str(max(0, cfg.scheduler_priority))])
        srun_args.extend(map(str, task.scheduler_extra_args))
        args = list(map(str, task.program_args()))
        env = task.program_environment()
        logger.info('Running Slurm command {}'.format(' '.join(srun_args + args)))
        proc = Popen(srun_args + args, env=env, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        with ExternalProgramRunContext(proc):
            stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise ExternalProgramRunError('Program exited with non-zero return code.', tuple(args), env, stdout, stderr)
        if task.capture_output:
            logger.info('Program stdout:\n{}'.format(stdout))
            logger.info('Program stderr:\n{}'.format(stderr))
