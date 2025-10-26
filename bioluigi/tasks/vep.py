from typing import Optional

import luigi

from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

class Annotate(ScheduledExternalProgramTask):
    """
    Annotate a VCF file with Ensembl VEP.

    Setting :cpus: parameter will adjust the number of forks (via --fork) VEP
    uses to annotate its input.
    """
    task_namespace = 'vep'

    vcf_file = luigi.Parameter()
    annotated_vcf_file = luigi.Parameter()

    cache = luigi.BoolParameter(default=False, positional=False, significant=False)
    offline = luigi.BoolParameter(default=False, positional=False, significant=False)

    buffer_size = luigi.IntParameter(default=5000, positional=False, significant=False)

    species = luigi.Parameter(positional=False)
    assembly = luigi.Parameter(positional=False)

    plugins: list[str] = luigi.ListParameter(default=[], positional=False)

    extra_args: list[str] = luigi.ListParameter(default=[])

    output_format = luigi.ChoiceParameter(choices=['vcf', 'tab', 'json'], default='vcf')
    compress_output = luigi.ChoiceParameter(choices=['gzip', 'bgzip'], default='bgzip')

    _tmp_output_file: Optional[str] = None

    def program_args(self):
        args = [cfg.vep_bin,
                '-i', self.vcf_file,
                '--format', 'vcf',
                '--fork', self.cpus,
                '--buffer_size', self.buffer_size,
                '--species', self.species,
                '--assembly', self.assembly,
                f'--{self.output_format}',
                '--compress_output', self.compress_output,
                '--output_file', self._tmp_output_file if self._tmp_output_file else self.annotated_vcf_file]

        if self.cache:
            args.append('--cache')

        if self.offline:
            args.append('--offline')

        if cfg.vep_dir is not None:
            args.extend(['--dir', cfg.vep_dir])

        for plugin in self.plugins:
            args.extend(['--plugin', plugin])

        args.extend(self.extra_args)

        return args

    def run(self):
        with self.output().temporary_path() as self._tmp_output_file:
            super().run()

    def output(self):
        return LocalTarget(self.annotated_vcf_file)
