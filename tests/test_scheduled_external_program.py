import tempfile
from os.path import join, dirname, abspath

import luigi
import luigi.parameter

from bioluigi.scheduled_external_program import ScheduledExternalProgramTask
from bioluigi.schedulers import get_available_schedulers, SshSchedulerConfig, SlurmSchedulerConfig, get_scheduler

testdir = dirname(__file__)
tmp_dir = tempfile.mkdtemp()

class MyTask(ScheduledExternalProgramTask):
    cpus = 4
    unique_id: str = luigi.Parameter(visibility=luigi.parameter.ParameterVisibility.PRIVATE)

    def program_args(self):
        return ['touch', self.output().path]

    def output(self):
        return luigi.LocalTarget(join(tmp_dir, self.unique_id))

def test_default_scheduler():
    task = MyTask("1")
    assert task.scheduler == 'local'
    assert task.scheduler_partition is None
    assert task.resources['cpus'] == 4
    assert not task.complete()
    luigi.build([task], local_scheduler=True)
    assert task.complete()

def test_local_scheduler():
    assert 'local' in get_available_schedulers()
    task = MyTask("2", scheduler='local')
    assert task.resources['cpus'] == 4
    assert task.resources['memory'] == 1
    assert not task.complete()
    luigi.build([task], local_scheduler=True)
    assert task.complete()

def test_slurm_scheduler():
    slurm_cfg = SlurmSchedulerConfig()
    slurm_cfg.srun_bin = abspath(join(testdir, 'srun-mock'))
    slurm_cfg.squeue_bin = abspath(join(testdir, 'squeue-mock'))
    assert 'slurm' in get_available_schedulers()
    task = MyTask("3", scheduler='slurm')
    assert 'slurm_jobs' in task.resources
    assert 'slurm_cpus' in task.resources
    assert task.resources['slurm_jobs'] == 1
    assert task.resources['slurm_cpus'] == 4
    assert not task.complete()
    slurm_scheduler = get_scheduler('slurm')
    assert slurm_scheduler.get_task_status_message(task) == '''{
    "comment": "MyTask__99914b932b"
}'''
    luigi.build([task], local_scheduler=True)
    assert task.complete()

def test_ssh_scheduler():
    ssh_cfg = SshSchedulerConfig()
    ssh_cfg.ssh_bin = abspath(join(testdir, 'ssh-mock'))
    task = MyTask('5', scheduler='ssh')
    assert task.resources == {'ssh_cpus': 4, 'ssh_memory': 1}
    luigi.build([task], local_scheduler=True)
    assert task.complete()
