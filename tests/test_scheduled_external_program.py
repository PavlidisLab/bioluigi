import os
import luigi
from bioluigi.scheduled_external_program import ScheduledExternalProgramTask, Scheduler, SlurmScheduler
from distutils.spawn import find_executable
import pytest

class MyTask(ScheduledExternalProgramTask):
    def program_args(self):
        return ['true']

def test_default_scheduler():
    assert MyTask().scheduler == 'local'
    assert MyTask().scheduler_partition == ''
    luigi.build([MyTask()], local_scheduler=True)

def test_local_scheduler():
    luigi.build([MyTask(scheduler='local')], local_scheduler=True)

def test_slurm_scheduler():
    if find_executable('srun') is None:
        pytest.skip('srun is needed to run Slurm tests.')
    assert 'slurm_jobs' in MyTask(scheduler='slurm').resources
    assert MyTask(scheduler='slurm').resources['slurm_jobs'] == 1
    luigi.build([MyTask(scheduler='slurm')], local_scheduler=True)
