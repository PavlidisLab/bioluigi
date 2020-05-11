import luigi
from bioluigi.tasks.utils import TaskWithOutputMixin, DynamicWrapperTask, DynamicTaskWithOutputMixin

def test_task_with_output_mixin():
    class Req(luigi.Task):
        def output(self):
            return luigi.LocalTarget('this-output')

    class Tsk(TaskWithOutputMixin, luigi.Task):
        def requires(self):
            return Req()

    task = Tsk()
    assert isinstance(task.output(), luigi.LocalTarget)
    assert task.output().path == 'this-output'

def test_dynamic_wrapper_task():
    class Req(luigi.Task):
        def output(self):
            return luigi.LocalTarget('this-output')

    class Dyn(DynamicTaskWithOutputMixin, DynamicWrapperTask):
        def run(self):
            yield Req()

    dyn_task = Dyn()
    assert isinstance(dyn_task.output(), luigi.LocalTarget)
    assert dyn_task.output().path == 'this-output'
