from shutil import which

import luigi
import luigi.parameter
import pytest

from bioluigi.scheduled_external_program import ScheduledExternalProgramTask

class MyTask(ScheduledExternalProgramTask):
    cpus = 4
    unique_id = luigi.Parameter(visibility=luigi.parameter.ParameterVisibility.PRIVATE)

    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)
        self._completed = False

    def program_args(self):
        return ['true']

    def run(self):
        ret = super().run()
        self._completed = True
        return ret;

    def complete(self):
        return self._completed

def test_default_scheduler():
    task = MyTask("1")
    assert task.scheduler == 'local'
    assert task.scheduler_partition == ''
    assert task.resources['cpus'] == 4
    assert not task.complete()
    luigi.build([task], local_scheduler=True)
    assert task.complete()

def test_local_scheduler():
    task = MyTask("2", scheduler='local')
    assert not task.complete()
    luigi.build([task], local_scheduler=True)
    assert task.complete()

def test_slurm_scheduler():
    if which('srun') is None:
        pytest.skip('srun is needed to run Slurm tests.')
    task = MyTask("3", scheduler='slurm')
    assert 'slurm_jobs' in task.resources
    assert 'slurm_cpus' in task.resources
    assert task.resources['slurm_jobs'] == 1
    assert task.resources['slurm_cpus'] == 4
    assert not task.complete()
    luigi.build([task], local_scheduler=True)
    assert task.complete()
