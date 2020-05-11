"""
Collection of utilities and mixins for tasks
"""

import logging

import luigi
from luigi.task import getpaths, flatten, flatten_output

logger = logging.getLogger('luigi-interface')

class DynamicWrapperTask(luigi.Task):
    """
    Similar to luigi.task.WrapperTask but for dynamic dependencies yielded in
    the body of the run() method.
    """
    def complete(self):
        # ensure that static dependencies are met
        if not all(tasks.complete() for task in self.requires()):
            return False

        # ensure that all dynamic dependencies are met
        return all(req.complete() for chunk in self.run()
                   for req in flatten(chunk))

class TaskWithOutputMixin(object):
    """
    Extends a luigi.WrapperTask to forward its dependencies as output.
    """
    def output(self):
        return getpaths(self.requires())

class DynamicTaskWithOutputMixin(object):
    """
    Extends a task to forward its dynamic dependencies as output.
    """
    def output(self):
        tasks = []
        if all(req.complete() for req in flatten(self.requires())):
            try:
                tasks = list(self.run())
            except:
                logger.exception('%s failed at run() step; the exception will not be raised because Luigi is still building the graph.', repr(self))

        # FIXME: conserve task structure: the generator actually create an
        # implicit array level even if a single task is yielded.
        # For now, we just handle the special singleton case.
        if len(tasks) == 1:
            tasks = tasks[0]

        return getpaths(tasks)

class RemoveTaskOutputOnFailureMixin(object):
    """
    Extends a task to remove its outputs on failure.
    """
    def on_failure(self, err):
        logger.info('Removing task output of {} due to failure...', repr(self))
        for out in flatten_output(self):
            if out.exists() and hasattr(out, 'remove'):
                out.remove()
        return super(RemoveTaskOutputOnFailureMixin, self).on_failure(err)
