"""
Collection of utilities and mixins for tasks
"""

import logging

import luigi
from luigi.parameter import DictParameter
from luigi.task import getpaths, flatten, flatten_output

logger = logging.getLogger(__name__)

class DynamicWrapperTask(luigi.Task):
    """
    Similar to luigi.task.WrapperTask but for dynamic dependencies yielded in
    the body of the run() method.
    """

    def complete(self):
        # ensure that static dependencies are met
        if not all(req.complete() for req in flatten(self.requires())):
            return False

        # ensure that all dynamic dependencies are met
        try:
            return all(req.complete() for chunk in self.run()
                       for req in flatten(chunk))
        except:
            logger.exception(
                '%s failed at run() step; the exception will not be raised because Luigi is still building the graph.',
                repr(self))
            return False

class TaskWithOutputMixin(luigi.Task):
    """
    Extends a luigi.WrapperTask to forward its dependencies as output.
    """

    def output(self):
        return getpaths(self.requires())

class DynamicTaskWithOutputMixin(luigi.Task):
    """
    Extends a task to forward its dynamic dependencies as output.
    """
    unpack_singleton = True

    def output(self):
        tasks = []
        if all(req.complete() for req in flatten(self.requires())):
            try:
                tasks = list(self.run())
            except:
                logger.exception(
                    '%s failed at run() step; the exception will not be raised because Luigi is still building the graph.',
                    repr(self))

        # FIXME: conserve task structure: the generator actually create an
        # implicit array level even if a single task is yielded.
        # For now, we just handle the special singleton case.
        if self.unpack_singleton and len(tasks) == 1:
            tasks = tasks[0]

        return getpaths(tasks)

class CreateTaskOutputDirectoriesBeforeRunMixin(luigi.Task):
    """
    Ensures that output directories exist before running the task.
    """

    def run(self):
        for out in flatten_output(self):
            if hasattr(out, 'makedirs'):
                out.makedirs()
        return super().run()

class RemoveTaskOutputOnFailureMixin(luigi.Task):
    """
    Remove a task outputs on failure.

    This only applies for output that have a defined 'remove' method.
    """

    def on_failure(self, err):
        logger.info('Removing task output of %s due to failure.', repr(self))
        for out in flatten_output(self):
            if out.exists() and hasattr(out, 'remove'):
                try:
                    out.remove()
                except:
                    logger.exception('Failed to remove output %s while cleaning up %s.', repr(out), repr(self))
        return super().on_failure(err)

class TaskWithMetadataMixin:
    """
    Mixin that adds an insignificant metadata parameter to a task.
    """
    metadata = DictParameter(default={}, positional=False, significant=False)
