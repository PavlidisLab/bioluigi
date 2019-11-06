import luigi
from luigi.contrib.external_program import ExternalProgramTask
import os

class IndexBam(ExternalProgramTask):
    bam_file = luigi.Parameter()

    def program_args(self):
        return ['samtools', 'index', self.bam_file]

    def output(self):
        return luigi.LocalTarget('{}.bai'.format(self.bam_file))
