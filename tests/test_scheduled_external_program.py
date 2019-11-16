import os
import luigi
from bioluigi.scheduled_external_program import ScheduledExternalProgramTask
from distutils.spawn import find_executable
from nose.plugins.skip import SkipTest

class MyTask(ScheduledExternalProgramTask):
    def program_args(self):
        return ['true']

def test_default_scheduler():
    assert MyTask().scheduler == 'local'
    luigi.build([MyTask()], local_scheduler=True)

def test_local_scheduler():
    luigi.build([MyTask(scheduler='local')], local_scheduler=True)

def test_slurm_scheduler():
    if find_executable('srun') is None:
        raise SkipTest('srun is needed to run Slurm tests.')
    luigi.build([MyTask(scheduler='slurm')], local_scheduler=True)
