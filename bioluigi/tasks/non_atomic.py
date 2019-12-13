import luigi
from luigi.task import flatten

class NonAtomicTaskRunContext(object):
    """
    Execution context for non-atomic tasks that ensures that any existing
    output is deleted if the task fails.
    """
    def __init__(self, task):
        self.task = task

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            for out in flatten(self.task.output()):
                if out.exists() and hasattr(out, 'remove'):
                    out.remove()

def non_atomic(cls):
    class Wrapper(cls):
        def run(self):
            with NonAtomicTaskRunContext(self):
                return super(Wrapper, self).run()
    return Wrapper
