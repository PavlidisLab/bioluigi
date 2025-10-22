"""
This module provides a flavour of :class:`luigi.contrib.external_program.ExternalProgramTask`
designed to work with an external scheduler such as Slurm.

It also extend the basic definition of an external task to declare basic
resource consumptions such as the number of CPU and the amount of memory
required to execute the task.
"""

import datetime
from datetime import timedelta
from typing import Optional

import luigi
from luigi.contrib.external_program import ExternalProgramTask

from .config import bioluigi
from .schedulers import get_available_schedulers, get_scheduler, ScheduledTask

cfg = bioluigi()

class ScheduledExternalProgramTask(ExternalProgramTask, ScheduledTask):
    """
    Variant of :class:`luigi.contrib.external_program.ExternalProgramTask` that
    executes the task with a :class:`Scheduler`.
    """
    scheduler: str = luigi.ChoiceParameter(default=cfg.scheduler,
                                           choices=['local'] + [blurb for blurb in get_available_schedulers()],
                                           positional=False, significant=False,
                                           description='Scheduler to use for running the task')
    scheduler_partition: Optional[str] = luigi.OptionalParameter(default=cfg.scheduler_partition, positional=False,
                                                                 significant=False,
                                                                 description='Scheduler partition (or queue) to use if supported')
    scheduler_extra_args: list[str] = luigi.ListParameter(default=cfg.scheduler_extra_args, positional=False,
                                                          significant=False,
                                                          description='Extra arguments to pass to the scheduler')

    walltime: timedelta = luigi.TimeDeltaParameter(default=datetime.timedelta(), positional=False, significant=False,
                                                   description='Amount of time to allocate for the task, default value of zero implies unlimited time')
    cpus: int = luigi.IntParameter(default=1, positional=False, significant=False,
                                   description='Number of CPUs to allocate for the task')
    memory: float = luigi.FloatParameter(default=1, positional=False, significant=False,
                                         description='Amount of memory (in gigabyte) to allocate for the task')

    @property
    def resources(self):
        if self.scheduler == 'local':
            # local_jobs is actually constrained by the number of workers
            return {'cpus': self.cpus, 'memory': self.memory}
        else:
            return {'{}_jobs'.format(self.scheduler): 1, '{}_cpus'.format(self.scheduler): self.cpus}

    def run(self):
        if self.scheduler == 'local':
            super().run()
        else:
            try:
                _scheduler = get_scheduler(self.scheduler)
            except KeyError:
                raise ValueError('Unsupported scheduler {}'.format(self.scheduler))
            _scheduler.run_task(self)

