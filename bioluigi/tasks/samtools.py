import luigi
from luigi.contrib.external_program import ExternalProgramTask

from ..config import BioluigiConfig
from ..local_target import LocalTarget

cfg = BioluigiConfig()

class IndexBam(ExternalProgramTask):
    bam_file = luigi.Parameter()

    def program_args(self):
        return ['samtools', 'index', self.bam_file]

    def output(self):
        return LocalTarget('{}.bai'.format(self.bam_file))
