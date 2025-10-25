import json
import logging
import os
import shlex
import signal
import subprocess
import tempfile
import threading
import time
from abc import abstractmethod, ABC
from datetime import timedelta
from os.path import join
from subprocess import Popen
from typing import Optional, Any, Union

import luigi
from build import env
from luigi.contrib.external_program import ExternalProgramRunError, ExternalProgramRunContext

from .config import bioluigi

logger = logging.getLogger(__name__)

cfg = bioluigi()

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

class ScheduledArrayTask(ScheduledTask, ABC):
    """Interface for Luigi task that can be scheduled as an array of similar jobs."""

    array: Union[list, range]
    """Array of parameters to schedule the external program with."""

    array_task_batch_size: Optional[int]
    """Indicate how many tasks can be run concurrently. The default is to run all tasks at once."""

    def program_args(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def program_args_for_array_element(self, element, index):
        """Obtain the program arguments for a particular element of the array task."""
        pass

    @abstractmethod
    def program_environment_for_array_element(self, element, index):
        """Obtain the program environment for a particular element of the array task."""
        pass

    @abstractmethod
    def working_directory_for_array_element(self, element, index):
        """Obtain the working directory for a particular element of the array task."""
        pass

class Scheduler(ABC):
    @abstractmethod
    def get_resources_for_task(self, task: ScheduledTask) -> dict[str, int]:
        """Obtain the resources for a particular task."""
        pass

    @abstractmethod
    def get_resources_for_array_task(self, task: ScheduledArrayTask) -> dict[str, int]:
        """Obtain the resources for a particular array task."""
        pass

    @abstractmethod
    def run_task(self, task: ScheduledTask):
        """Run a scheduled task."""
        pass

    @abstractmethod
    def run_array_task(self, task: ScheduledArrayTask):
        """Run a scheduled array task for a particular set of parameters."""
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
_TASK_STATUS_UPDATE_FREQUENCY: float = 1

class BaseScheduler(Scheduler, ABC):
    @staticmethod
    def _run_command(args, env, cwd, capture_output):
        kwargs = {'env': env, 'text': True}
        if capture_output:
            kwargs['stdout'] = subprocess.PIPE
            kwargs['stderr'] = subprocess.PIPE
        if cwd:
            kwargs['cwd'] = cwd
        logger.info(args)
        logger.info('Running command %s%s', ' '.join(args), 'in ' + cwd if cwd else '')
        return Popen(args, **kwargs)

    def _wait_for_command(self, task: ScheduledTask, proc: subprocess.Popen, args, env, capture_output):
        stdout, stderr = None, None
        with ExternalProgramRunContext(proc):
            last_tracking_url, last_task_status_message, last_progress_percentage = None, None, None
            while True:
                if tracking_url := self.get_task_tracking_url(task):
                    if tracking_url != last_tracking_url:
                        task.set_tracking_url(tracking_url)
                if status_message := self.get_task_status_message(task):
                    if status_message != last_task_status_message:
                        task.set_status_message(status_message)
                if progress_percentage := self.get_task_progress_percentage(task) is not None:  # might also be zero
                    if progress_percentage != last_progress_percentage:
                        task.set_progress_percentage(progress_percentage)
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

    srun_bin: str = luigi.Parameter(default='srun')
    sbatch_bin: str = luigi.Parameter(default='sbatch')
    squeue_bin: str = luigi.Parameter(default='squeue')
    scancel_bin: str = luigi.Parameter(default='scancel')
    sacct_bin: str = luigi.Parameter('sacct')
    job_state_dir: Optional[str] = luigi.OptionalParameter(default=None,
                                                           description='Directory to use to store job states.')
    job_submission_method: str = luigi.ChoiceParameter(default='srun', choices=['srun', 'sbatch'],
                                                       description='Preferred job submission method. The default is to use srun.')
    partition: Optional[str] = luigi.OptionalParameter(default=None)
    extra_args: list[str] = luigi.ListParameter(default=[])

slurm_cfg = SlurmSchedulerConfig()

@register_scheduler('slurm')
class SlurmScheduler(BaseScheduler):
    """
    Scheduler based on Slurm https://slurm.schedmd.com/
    """

    def get_resources_for_task(self, task: ScheduledTask):
        return {'slurm_jobs': 1, 'slurm_cpus': task.cpus}

    def get_resources_for_array_task(self, task: ScheduledArrayTask) -> dict[str, int]:
        # job arrays are allocated resources for the "first" job in the array
        return {'slurm_jobs': 1, 'slurm_cpus': task.cpus}

    @staticmethod
    def get_slurm_job_array_ids_for_array_task(task: ScheduledArrayTask) -> list[int]:
        """Obtain the Slurm job array IDs that would result from a particular array task."""
        if isinstance(task.array, range):
            return [i + 1 for i in task.array]
        else:
            return [i + 1 for i in range(len(task.array))]

    @staticmethod
    def get_slurm_args_for_task(task: ScheduledTask):
        secs = int(task.walltime.total_seconds())
        args = [
            '--verbose',
            '--job-name', task.get_task_family(),
            '--comment', task.task_id,
            '--time',
            '{}-{:02d}:{:02d}:{:02d}'.format(secs // 86400, (secs % 86400) // 3600, (secs % 3600) // 60, secs % 60),
            '--mem', '{}G'.format(int(task.memory)),
            '--cpus-per-task', str(task.cpus)]
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
        return args

    @classmethod
    def get_slurm_args_for_array_task(cls, task: ScheduledArrayTask, tmp_dir):
        args = cls.get_slurm_args_for_task(task)
        if isinstance(task.array, range):
            args.extend(['--array', f'{task.array.start + 1}-{task.array.stop}:{task.array.step}'])
        else:
            args.extend(['--array', f'1:{len(task.array)}'])
        if task.capture_output:
            args.extend(['--output', join(tmp_dir, 'stdout-%a'), '--error', join(tmp_dir, 'stderr-%a')])
        return args

    @staticmethod
    def get_slurm_script_for_task(task: ScheduledTask, tmp_dir):
        script = '#!/bin/sh\n'
        # environment is already inherited from the subprocess
        script += ' '.join(shlex.quote(arg) for arg in task.program_args()) + '\n'
        script += 'echo $? > ' + shlex.quote(join(tmp_dir, 'exit-status')) + '\n'
        print(script)
        return script

    @staticmethod
    def get_slurm_script_for_array_task(task: ScheduledArrayTask, tmp_dir):
        script = '#!/bin/sh\n'
        script += 'case "$SLURM_ARRAY_TASK_ID" in\n'
        for i, arg in enumerate(task.array):
            INDENT = '    '
            script += INDENT + f'{i + 1})\n'
            cwd = task.working_directory_for_array_element(arg, i)
            if cwd:
                script += INDENT + 'cd ' + shlex.quote(cwd) + '\n'
            script += '    export ' + ' '.join('{}={}'.format(k, shlex.quote(v)) for k, v in
                                               task.program_environment_for_array_element(arg, i).items()) + '\n'
            script += INDENT + ' '.join(
                shlex.quote(str(arg)) for arg in task.program_args_for_array_element(arg, i)) + '\n'
            # there is also a stderr-%j and stdout-%j file, but those are declared in the sbatch args
            script += INDENT + 'echo $? > ' + shlex.quote(join(tmp_dir, 'exit-status')) + '\n'
            script += INDENT + ';;\n'
        script += 'esac'
        return script

    # cached output of squeue
    _squeue_cache_lock: threading.Lock = threading.Lock()
    _squeue_cache: str = None
    _squeue_cache_last_updated: float = None

    @classmethod
    def get_slurm_job_details_for_task(cls, task: ScheduledTask,
                                       ttl: float,
                                       job_id: Optional[int] = None) -> Optional[dict[str, Any]]:
        """Obtain the Slurm job details for a given Luigi task"""
        with cls._squeue_cache_lock:
            if cls._squeue_cache is None or time.time() > (cls._squeue_cache_last_updated + ttl):
                cls._squeue_cache = subprocess.run([slurm_cfg.squeue_bin, '--json'], stdout=subprocess.PIPE,
                                                   text=True).stdout
                cls._squeue_cache_last_updated = time.time()
        try:
            payload = json.loads(cls._squeue_cache)
        except json.JSONDecodeError:
            logger.exception('Failed to decode squeue JSON output.')
            return None
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
            # if an explicit job ID is provided, use it
            if job_id and 'job_id' in job_def and job_def['job_id'] == job_id:
                return job_def
            if 'comment' not in job_def:
                continue
            if not isinstance(job_def['comment'], str):
                logger.error('Expected $.jobs[%d].comment to be a string.', job_def_i)
                return None
            if job_def['comment'] == task.task_id:
                return job_def
        logger.warning('Could not find job details for task with ID %s%s.', task.task_id,
                       ' and job ID ' + str(job_id) if job_id else '')
        return None

    def get_slurm_job_state_for_task(self, task: ScheduledTask, job_id=None) -> list[str]:
        job_details = self.get_slurm_job_details_for_task(task, ttl=_TASK_STATUS_UPDATE_FREQUENCY, job_id=job_id)
        return job_details['job_state'] if job_details and 'job_state' in job_details and isinstance(
            job_details['job_state'], list) else []

    def get_task_tracking_url(self, task: ScheduledTask) -> Optional[str]:
        # TODO: integrate Slurm Web https://slurm-web.com/
        return None

    def get_task_status_message(self, task: ScheduledTask) -> Optional[str]:
        job_def = self.get_slurm_job_details_for_task(task, ttl=_TASK_STATUS_UPDATE_FREQUENCY)
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

    def get_task_progress_percentage(self, task: ScheduledTask) -> Optional[int]:
        # TODO: check job array progress
        return None

    def _wait_for_slurm_job_to_complete(self, task: ScheduledTask, job_id: int):
        def cancel_job(captured_signal=None, stack_frame=None):
            subprocess.run([slurm_cfg.scancel_bin, str(job_id)], check=True)

        prev_sigterm_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, cancel_job)
        try:
            last_tracking_url, last_task_status_message, last_progress_percentage = None, None, None
            while 'RUNNING' in self.get_slurm_job_state_for_task(task, job_id):
                if tracking_url := self.get_task_tracking_url(task):
                    if tracking_url != last_tracking_url:
                        task.set_tracking_url(tracking_url)
                if status_message := self.get_task_status_message(task):
                    if status_message != last_task_status_message:
                        task.set_status_message(status_message)
                if progress_percentage := self.get_task_progress_percentage(task) is not None:  # might also be zero
                    if progress_percentage != last_progress_percentage:
                        task.set_progress_percentage(progress_percentage)
        except KeyboardInterrupt:
            cancel_job()
            raise
        finally:
            signal.signal(signal.SIGTERM, prev_sigterm_handler)

    def run_task(self, task: ScheduledTask):
        if slurm_cfg.job_submission_method == 'srun':
            self.run_task_with_srun(task)
        elif slurm_cfg.job_submission_method == 'sbatch':
            self.run_task_with_sbatch(task)
        else:
            raise NotImplementedError

    def run_task_with_srun(self, task: ScheduledTask):
        """Run a scheduled task with srun"""
        args = [slurm_cfg.srun_bin]
        args.extend(self.get_slurm_args_for_task(task))
        args.extend(map(str, task.program_args()))
        env = task.program_environment()
        proc = self._run_command(args, env, task.working_directory, task.capture_output)
        self._wait_for_command(task, proc, args, env, task.capture_output)

    def run_task_with_sbatch(self, task: ScheduledTask):
        """Run a scheduled task with sbatch"""
        if not slurm_cfg.job_state_dir:
            raise RuntimeError('A job state directory must be configured to use sbatch.')
        with tempfile.TemporaryDirectory(dir=slurm_cfg.job_state_dir) as tmp_dir:
            args = [slurm_cfg.sbatch_bin]
            args.extend(self.get_slurm_args_for_task(task))
            if task.capture_output:
                args.extend(['--output', join(tmp_dir, 'stdout'), '--error', join(tmp_dir, 'stderr')])
            job_file = join(tmp_dir, 'job.sh')
            with open(job_file, 'w') as f:
                f.write(self.get_slurm_script_for_task(task, tmp_dir))
            os.chmod(job_file, 0o755)
            args.append(job_file)
            env = task.program_environment()
            proc = self._run_command(args, env, task.working_directory, task.capture_output)
            with ExternalProgramRunContext(proc):
                stdout, stderr = proc.communicate()
                job_id = int(stdout.strip())
            self._wait_for_slurm_job_to_complete(task, job_id)

            if task.capture_output:
                try:
                    with open(join(tmp_dir, 'stdout')) as f:
                        stdout = f.read()
                except FileNotFoundError:
                    stdout = None
                try:
                    with open(join(tmp_dir, 'stderr')) as f:
                        stderr = f.read()
                except FileNotFoundError:
                    stderr = None
                if stdout:
                    logger.info('Program stdout:\n%s', stdout)
                if stderr:
                    logger.info('Program stderr:\n%s', stderr)

            with open(join(tmp_dir, 'exit-status')) as f:
                exit_status = int(f.read().strip())
            if exit_status != 0:
                raise ExternalProgramRunError('', tuple(args), env, stdout, stderr)

    def run_array_task(self, task: ScheduledArrayTask):
        """
        Scheduled array task are always run with sbatch, so a temporary directory has to be created.
        https://slurm.schedmd.com/job_array.html
        """
        if not slurm_cfg.job_state_dir:
            raise RuntimeError('A job state directory must be configured to use sbatch.')
        with tempfile.TemporaryDirectory(dir=slurm_cfg.job_state_dir) as tmp_dir:
            args = [slurm_cfg.sbatch_bin]
            args.extend(self.get_slurm_args_for_array_task(task, tmp_dir))
            slurm_script = self.get_slurm_script_for_array_task(task, tmp_dir)
            job_file = join(tmp_dir, 'job.sh')
            with open(job_file, 'w') as f:
                f.write(slurm_script)
            os.chmod(job_file, 0x755)
            args.append(job_file)
            proc = self._run_command(args=args, env=None, cwd=None, capture_output=True)
            with ExternalProgramRunContext(proc):
                stdout, stderr = proc.communicate()
                job_id = int(stdout.strip())

            self._wait_for_slurm_job_to_complete(task, job_id)

            stdout, stderr = None, None
            if task.capture_output:
                for element, array_id in zip(task.array, self.get_slurm_job_array_ids_for_array_task(task)):
                    try:
                        with open(join(tmp_dir, 'stdout-' + str(array_id))) as f:
                            stdout = f.read()
                    except FileNotFoundError:
                        stdout = None
                    try:
                        with open(join(tmp_dir, 'stderr-' + str(array_id))) as f:
                            stderr = f.read()
                    except FileNotFoundError:
                        stderr = None
                    if stdout:
                        logger.info('Program[%s] stdout:\n%s', element, stdout)
                    if stderr:
                        logger.info('Program[%s] stderr:\n%s', element, stderr)

            exceptions = []
            for array_id in self.get_slurm_job_array_ids_for_array_task(task):
                with open(join(tmp_dir, 'exit-status-' + str(array_id))) as f:
                    exit_status = int(f.read().strip())
                if exit_status != 0:
                    exceptions.append(ExternalProgramRunError('Program exited with non-zero return code.',
                                                              tuple(args), env, stdout, stderr))

            if exceptions:
                raise exceptions[0]

@register_scheduler('local')
class LocalScheduler(BaseScheduler):
    """A scheduler that uses a local subprocess"""

    def get_resources_for_task(self, task: ScheduledTask):
        return {'cpus': task.cpus, 'memory': task.memory}

    def get_resources_for_array_task(self, task: ScheduledArrayTask):
        if task.array_task_batch_size:
            n = min(task.array_task_batch_size, len(task.array))
        else:
            n = len(task.array)
        return {'cpus': task.cpus * n, 'memory': task.memory * n}

    def run_task(self, task: ScheduledTask):
        args = list(map(str, task.program_args()))
        env = task.program_environment()
        proc = self._run_command(args, env, task.working_directory, task.capture_output)
        self._wait_for_command(task, proc, args, env, task.capture_output)

    def run_array_task(self, task: ScheduledArrayTask):
        command_defs = [(list(map(str, task.program_args_for_array_element(element, index))),
                         task.program_environment_for_array_element(element, index),
                         task.working_directory_for_array_element(element, index),
                         task.capture_output)
                        for index, element in enumerate(task.array)]
        max_procs = min(task.array_task_batch_size, len(command_defs)) if task.array_task_batch_size \
            else len(command_defs)

        procs = []
        exceptions = []

        def destroy_procs(captured_signal=None, stack_frame=None):
            for _, proc in procs:
                proc.kill()

        prev_sigterm_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, destroy_procs)
        try:
            while command_defs:
                # check for completed subprocesses
                # going reverse so pop(i) is always valid
                for i, (cd, proc) in reversed(list(enumerate(procs))):
                    if proc.poll() is not None:
                        procs.pop(i)
                        if task.capture_output:
                            stdout, stderr = proc.communicate()
                            if stdout:
                                logger.info('Program stdout:\n%s', stdout)
                            if stderr:
                                logger.info('Program stderr:\n%s', stderr)
                        else:
                            proc.wait()
                            stdout, stderr = None, None
                        if proc.returncode != 0:
                            args, env, *_ = cd
                            exceptions.append(ExternalProgramRunError('Program exited with non-zero return code.',
                                                                      args, env, stdout, stderr))
                # schedule more subprocesses
                while len(procs) < max_procs:
                    cd = command_defs.pop()
                    args, env, cwd, co = cd
                    procs.append((cd, self._run_command(args, env, cwd, co)))
        except KeyboardInterrupt:
            destroy_procs()
            raise
        finally:
            # wait for each remaining processes
            for _, proc in procs:
                proc.wait()
            signal.signal(signal.SIGTERM, prev_sigterm_handler)
        if exceptions:
            # TODO: use an ExceptionGroup to include all exceptions
            raise exceptions[0]

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

    def get_resources_for_array_task(self, task: ScheduledArrayTask) -> dict[str, int]:
        raise NotImplementedError

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
        env = task.program_environment()
        proc = self._run_command(args, env, task.working_directory, task.capture_output)
        self._wait_for_command(task, proc, args, env, task.capture_output)

    def run_array_task(self, task: ScheduledArrayTask):
        raise NotImplementedError
