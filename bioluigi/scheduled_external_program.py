"""
This module provides a flavour of :class:`luigi.contrib.external_program.ExternalProgramTask`
designed to work with an external scheduler such as Slurm.

It also extends the basic definition of an external task to declare basic
resource consumptions such as the number of CPU and the amount of memory
required to execute the task.
"""

import datetime
import os
from abc import ABC
from datetime import timedelta
from typing import Optional

import luigi

from .config import BioluigiConfig
from .schedulers import get_available_schedulers, get_scheduler, ScheduledTask

cfg = BioluigiConfig()

class ScheduledExternalProgramTask(ScheduledTask, ABC):
    """
    Variant of :class:`luigi.contrib.external_program.ExternalProgramTask` that
    executes the task with a :class:`Scheduler`.
    """
    scheduler: str = luigi.ChoiceParameter(default=cfg.scheduler,
                                           choices=[blurb for blurb in get_available_schedulers()],
                                           positional=False, significant=False,
                                           description='Scheduler to use for running the task (defaults to bioluigi.scheduler)')
    scheduler_partition: Optional[str] = luigi.OptionalParameter(default=cfg.scheduler_partition, positional=False,
                                                                 significant=False,
                                                                 description='Scheduler partition (or queue) to use if supported (defaults to bioluigi.scheduler_partition)')
    scheduler_extra_args: list[str] = luigi.ListParameter(default=cfg.scheduler_extra_args, positional=False,
                                                          significant=False,
                                                          description='Extra arguments to pass to the scheduler (defaults to bioluigi.scheduler_extra_args)')

    walltime: timedelta = luigi.TimeDeltaParameter(default=datetime.timedelta(), positional=False, significant=False,
                                                   description='Amount of time to allocate for the task, default value of zero implies unlimited time')
    cpus: int = luigi.IntParameter(default=1, positional=False, significant=False,
                                   description='Number of CPUs to allocate for the task')
    memory: float = luigi.FloatParameter(default=1, positional=False, significant=False,
                                         description='Amount of memory (in gigabyte) to allocate for the task')

    working_directory = luigi.OptionalParameter(default=None, significant=False, positional=False)

    capture_output = luigi.BoolParameter(default=True, significant=False, positional=False)

    def program_environment(self) -> dict[str, str]:
        env = os.environ.copy()
        return env

    @property
    def resources(self):
        return get_scheduler(self.scheduler).get_resources_for_task(self)

    def run(self):
        get_scheduler(self.scheduler).run_task(self)
