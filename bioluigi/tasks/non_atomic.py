import luigi

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
            for out in luigi.task.flatten_output(self.task):
                if out.exists():
                    out.remove()
