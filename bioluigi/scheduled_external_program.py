"""
This module provides a flavour of :class:`luigi.contrib.external_program.ExternalProgramTask`
designed to work with an external scheduler such as Slurm.

It also extend the basic definition of an external task to declare basic
resource consumptions such as the number of CPU and the amount of memory
required to execute the task.
"""

import luigi
from luigi.contrib.external_program import ExternalProgramTask, ExternalProgramRunError, ExternalProgramRunContext
from subprocess import Popen, PIPE, check_call
import os
import datetime
import logging

from .config import bioluigi

cfg = bioluigi()

logger = logging.getLogger('luigi-interface')

class Scheduler(object):
    """
    :param blurb: Short name by with the scheduler is referred to
    or if it should be done through Luigi's resource management system.
    """

    blurb = None

    @classmethod
    def fromblurb(cls, blurb):
        for subcls in cls.__subclasses__():
            if subcls.blurb == blurb:
                return subcls()
        else:
            raise ValueError('{} is not a recognized scheduler.'.format(blurb))

    @classmethod
    def run_task(self, task):
        raise NotImplementedError

class SlurmScheduler(Scheduler):
    """
    Scheduler based on Slurm https://slurm.schedmd.com/
    """
    blurb = 'slurm'

    @classmethod
    def run_task(self, task):
        secs = int(task.walltime.total_seconds())
        srun_args = ['srun']
        srun_args.extend([
            '--job-name', repr(task),
            '--time', '{}-{:02d}:{:02d}:{:02d}'.format(secs // 86400, (secs % 86400) // 3600, (secs % 3600) // 60, secs % 60),
            '--mem', '{}G'.format(int(task.memory)),
            '--cpus-per-task', str(task.cpus)])
        if cfg.scheduler_partition:
            srun_args.extend(['--partition', cfg.scheduler_partition])
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
            raise ExternalProgramRunError('Program exited with non-zero return code.', args, env, stdout, stderr)
        if task.capture_output:
            logger.info('Program stdout:\n{}'.format(stdout))
            logger.info('Program stderr:\n{}'.format(stderr))

class ScheduledExternalProgramTask(ExternalProgramTask):
    """
    Variant of :class:`luigi.contrib.external_program.ExternalProgramTask` that
    executes the task with a :class:`Scheduler`.
    """
    scheduler = luigi.ChoiceParameter(default=cfg.scheduler, choices=['local'] + [cls.blurb for cls in Scheduler.__subclasses__()], positional=False, significant=False, description='Scheduler to use for running the task')
    scheduler_partition = luigi.OptionalParameter(default=cfg.scheduler_partition, positional=False, significant=False, description='Scheduler partition (or queue) to use if supported')
    scheduler_extra_args = luigi.ListParameter(default=cfg.scheduler_extra_args, positional=False, significant=False, description='Extra arguments to pass to the scheduler')

    walltime = luigi.TimeDeltaParameter(default=datetime.timedelta(days=1), positional=False, significant=False, description='Amout of time to allocate for the task')
    cpus = luigi.IntParameter(default=1, positional=False, significant=False, description='Number of CPUs to allocate for the task')
    memory = luigi.FloatParameter(default=1, positional=False, significant=False, description='Amount of memory (in gigabyte) to allocate for the task')

    @property
    def resources(self):
        if self.scheduler == 'local':
            # local_jobs is actually constrained by the number of workers
            return {'cpus': self.cpus, 'memory': self.memory}
        else:
            return {'{}_jobs'.format(self.scheduler): 1}

    def run(self):
        if self.scheduler == 'local':
            return super(ScheduledExternalProgramTask, self).run()
        else:
            return Scheduler.fromblurb(self.scheduler).run_task(self)
