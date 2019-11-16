import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os.path import join

from ..config import bioluigi

cfg = bioluigi()

class Annotate(ExternalProgramTask):
    vcf_file = luigi.Parameter()
    annotated_vcf_file = luigi.Parameter()

    cache = luigi.BoolParameter(default=False, positional=False, significant=False)
    offline = luigi.BoolParameter(default=False, positional=False, significant=False)

    species = luigi.Parameter(positional=False)
    assembly = luigi.Parameter(positional=False)

    extra_args = luigi.ListParameter(default=[])

    def program_args(self):
        args = [cfg.vep_bin,
                '-i', self.vcf_file,
                '--format', 'vcf',
                '--species', self.species,
                '--assembly', self.assembly,
                '--vcf',
                '--compress_output', 'bgzip',
                '--output_file', self.output().path]

        if self.cache:
            args.append('--cache')

        if self.offline:
            args.append('--offline')

        if cfg.vep_cache_dir is not None:
            args.extend(['--dir', cfg.vep_cache_dir])

        args.extend(self.extra_args)

        return args

    def output(self):
        return luigi.LocalTarget(self.annotated_vcf_file)
