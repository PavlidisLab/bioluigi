import luigi

from scheduled_external_program import ScheduledExternalProgramTask
from ..config import BioluigiConfig
from ..local_target import LocalTarget

cfg = BioluigiConfig()

class IndexBam(ScheduledExternalProgramTask):
    bam_file = luigi.Parameter()

    _tmp_bam_file = None

    def program_args(self):
        return [cfg.samtools_bin, 'index', self._tmp_bam_file if self._tmp_bam_file else self.bam_file]

    def run(self):
        with self.output().temporary_path() as self._tmp_bam_file:
            super().run()

    def output(self):
        return LocalTarget('{}.bai'.format(self.bam_file))

class BamToFastq(ScheduledExternalProgramTask):
    bam_file = luigi.Parameter()
    r1_file = luigi.Parameter()
    r2_file = luigi.OptionalParameter(default=None)

    _tmp_r1_file = None
    _tmp_r2_file = None

    def program_args(self):
        args = [cfg.samtools_bin, 'fastq', '--threads', str(self.cpus), '-o', self.r1_file]
        if self.r2_file:
            args.extend(['-1', self._tmp_r1_file if self._tmp_r1_file else self.r1_file, '-2',
                         self._tmp_r2_file if self._tmp_r2_file else self.r2_file])
        else:
            args.extend(['-0', self._tmp_r1_file if self._tmp_r1_file else self.r1_file])
        args.append(self.bam_file)
        return args

    def run(self):
        if self.r2_file:
            with (self.output()[0].temporary_path() as self._tmp_r1_file,
                  self.output()[1].temporary_path() as self._tmp_r2_file):
                super().run()
        else:
            with self.output()[0].temporary_path() as self._tmp_bam_file:
                super().run()

    def output(self):
        outputs = [LocalTarget(self.r1_file)]
        if self.r2_file:
            outputs.append(LocalTarget(self.r2_file))
        return outputs
