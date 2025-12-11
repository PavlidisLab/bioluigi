import json
import logging
import subprocess
import threading
import time
from abc import abstractmethod, ABC
from datetime import timedelta
from subprocess import Popen
from typing import Optional

import luigi
from luigi.contrib.external_program import ExternalProgramRunError, ExternalProgramRunContext

from .config import BioluigiConfig

logger = logging.getLogger(__name__)

cfg = BioluigiConfig()

class ScheduledTask(luigi.Task, ABC):
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

class Scheduler(ABC):
    @abstractmethod
    def get_resources_for_task(self, task: ScheduledTask) -> dict[str, int]:
        pass

    @abstractmethod
    def run_task(self, task: ScheduledTask):
        pass

    @abstractmethod
    def get_task_tracking_url(self, task: ScheduledTask) -> Optional[str]:
        pass

    @abstractmethod
    def get_task_status_message(self, task: ScheduledTask) -> Optional[str]:
        pass

    @abstractmethod
    def get_task_progress_percentage(self, task: ScheduledTask) -> Optional[int]:
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

# in seconds
_TASK_STATUS_UPDATE_FREQUENCY: float = 10

class BaseScheduler(Scheduler):
    def _run_command(self, task: ScheduledTask, args, env, cwd, capture_output):
        kwargs = {'env': env, 'universal_newlines': True}
        if capture_output:
            kwargs['stdout'] = subprocess.PIPE
            kwargs['stderr'] = subprocess.PIPE
        if cwd:
            kwargs['cwd'] = cwd
        logger.info('Running command %s%s', ' '.join(args), 'in ' + cwd if cwd else '')
        proc = Popen(args, **kwargs)
        stdout, stderr = None, None
        with ExternalProgramRunContext(proc):
            if slurm_cfg.track_job_status:
                logger.info('Tracking task status updates from Slurm every %.1f seconds.',
                            _TASK_STATUS_UPDATE_FREQUENCY)
                last_tracking_url, last_status_message, last_progress_percentage = None, None, None
                while True:
                    # update task progress
                    if tracking_url := self.get_task_tracking_url(task):
                        if tracking_url != last_tracking_url:
                            task.set_tracking_url(tracking_url)
                            last_tracking_url = tracking_url
                    if status_message := self.get_task_status_message(task):
                        if status_message != last_status_message:
                            task.set_status_message(status_message)
                            last_status_message = status_message
                    if progress_percentage := self.get_task_progress_percentage(task) is not None:  # might also be zero
                        if progress_percentage != last_progress_percentage:
                            task.set_progress_percentage(progress_percentage)
                            last_progress_percentage = progress_percentage
                    try:
                        if capture_output:
                            stderr, stdout = proc.communicate(timeout=_TASK_STATUS_UPDATE_FREQUENCY)
                            if stdout:
                                logger.info('Program stdout:\n%s', stdout)
                            if stderr:
                                logger.info('Program stderr:\n%s', stderr)
                        else:
                            proc.wait(_TASK_STATUS_UPDATE_FREQUENCY)
                        break
                    except subprocess.TimeoutExpired:
                        continue
            else:
                if capture_output:
                    stderr, stdout = proc.communicate()
                    if stdout:
                        logger.info('Program stdout:\n%s', stdout)
                    if stderr:
                        logger.info('Program stderr:\n%s', stderr)
                else:
                    proc.wait()
        if proc.returncode != 0:
            raise ExternalProgramRunError('Program exited with non-zero return code.', tuple(args), env, stdout, stderr)

    def get_task_tracking_url(self, task: ScheduledTask) -> Optional[str]:
        return None

    def get_task_status_message(self, task: ScheduledTask) -> Optional[str]:
        return None

    def get_task_progress_percentage(self, task: ScheduledTask) -> Optional[int]:
        return None

class SlurmSchedulerConfig(luigi.Config):
    @classmethod
    def get_task_family(cls):
        return 'bioluigi.schedulers.slurm'

    srun_bin: str = luigi.Parameter(default='srun', description='Path to the srun executable.')
    squeue_bin: str = luigi.Parameter(default='squeue', description='Path to the squeue executable.')
    partition: Optional[str] = luigi.OptionalParameter(default=None,
                                                       description='Slurm partition to use by default. This can be overwritten by setting scheduler_partition in the task definition.')
    extra_args: list[str] = luigi.ListParameter(default=[],
                                                description='Extra arguments to pass to srun when submitting a job.')
    track_job_status: bool = luigi.BoolParameter(default=False,
                                                 description='Track job status update from Slurm. This is disabled by default because it is expensive.')

slurm_cfg = SlurmSchedulerConfig()

@register_scheduler('slurm')
class SlurmScheduler(BaseScheduler):
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
            '--comment', task.task_id,
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
        self._run_command(task, args=args, env=(task.program_environment()), cwd=None,
                          capture_output=task.capture_output)

    # cached output of squeue
    _squeue_lock = threading.Lock()
    _squeue_cache: dict = None
    _squeue_cache_last_updated: float = None

    @classmethod
    def _get_slurm_job_details(cls, task: ScheduledTask, ttl: float) -> Optional[dict]:
        with cls._squeue_lock:
            if cls._squeue_cache is None or time.time() > (cls._squeue_cache_last_updated + ttl):
                try:
                    cls._squeue_cache = json.loads(
                        subprocess.run([slurm_cfg.squeue_bin, '--json'], stdout=subprocess.PIPE,
                                       text=True).stdout)
                    cls._squeue_cache_last_updated = time.time()
                except json.JSONDecodeError:
                    logger.exception('Failed to decode squeue JSON output.')
                    return None
            else:
                logger.debug('Reusing cached squeue --json output')
        payload = cls._squeue_cache
        if not isinstance(payload, dict):
            logger.error('Expected $ to be a dictionary.')
            return None
        if not 'jobs' in payload:
            return None
        if not isinstance(payload['jobs'], list):
            logger.error('Expected $.jobs to be a list.')
            return None
        for job_def_i, job_def in enumerate(payload['jobs']):
            if not isinstance(job_def, dict):
                logger.error('Expected $.jobs[%d] to be a dictionary.', job_def_i)
                return None
            if 'comment' not in job_def:
                continue
            if not isinstance(job_def['comment'], str):
                logger.error('Expected $.jobs[%d].comment to be a string.', job_def_i)
                return None
            if job_def['comment'] == task.task_id:
                return job_def
        return None

    def get_task_status_message(self, task: ScheduledTask) -> Optional[str]:
        job_def = self._get_slurm_job_details(task, ttl=_TASK_STATUS_UPDATE_FREQUENCY)
        if job_def:
            keys = ["job_id", "job_state", "user_name", "user_id", "current_working_directory", "start_time",
                    "submit_time", "comment"]
            job_def_filtered = {}
            for key in keys:
                if key in job_def:
                    job_def_filtered[key] = job_def[key]
            return json.dumps(job_def_filtered, indent=4, sort_keys=True)
        else:
            return None

@register_scheduler('local')
class LocalScheduler(BaseScheduler):
    """A scheduler that uses a local subprocess"""

    def get_resources_for_task(self, task: ScheduledTask):
        return {'cpus': task.cpus, 'memory': task.memory}

    def run_task(self, task: ScheduledTask):
        self._run_command(task, args=list(map(str, task.program_args())), env=task.program_environment(),
                          cwd=task.working_directory,
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
class SshScheduler(BaseScheduler):
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
        self._run_command(task, args=args, env=task.program_environment(), cwd=task.working_directory,
                          capture_output=task.capture_output)
