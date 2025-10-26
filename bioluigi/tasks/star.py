import urllib
from os.path import join

import luigi

from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

class GenerateIndex(ScheduledExternalProgramTask):
    genome_fastas: list[str] = luigi.ListParameter()
    sjdb: str = luigi.Parameter()

    output_dir = luigi.Parameter()

    def program_args(self):
        return [cfg.star_bin,
                '--runCommand', 'generateGenome',
                '--genomeDir', self.output_dir]

class GenerateIndexFromEnsembl(luigi.Task):
    taxon = luigi.Parameter()
    version = luigi.IntParameter()

    def run(self):
        urllib.urlretrieve('ftp://', '')
        urllib.urlretrieve('ftp://', '')
        yield GenerateIndex()

class Align(ScheduledExternalProgramTask):
    """
    The task output the alignment and the splice junctions.

    :stranded: Whether the reads are strand-specific
    """
    fastqs: list[str] = luigi.ListParameter()
    genome_dir: str = luigi.Parameter()
    output_dir: str = luigi.Parameter()

    output_format: str = luigi.ChoiceParameter(choices=['sam', 'bam'], default='sam')
    sort_output: bool = luigi.BoolParameter()
    gzipped_reads: bool = luigi.BoolParameter(default=True)
    stranded_reads: bool = luigi.BoolParameter(default=False)

    # performance feature
    use_shared_memory = luigi.BoolParameter(default=True)

    cpu = 1
    mem = 32

    def program_args(self):
        args = [cfg.star_bin,
                '--runThreadN', self.resources['cpu'],
                '--genomeDir', self.genome_dir]

        if self.stranded_reads:
            args.extend(['--strand', 'Stranded'])

        if self.gzipped_reads:
            args.extend(['--readFileCommand', 'zcat'])

        if len(self.fastqs) == 1:
            pass
        elif len(self.fastqs) == 2:
            pass
        else:
            raise ValueError('')

        args.append('--readFilesIn')
        args.extend(self.fastqs)

        if self.use_shared_memory:
            args.extend(['--genomeLoad', 'LoadAndRemove'])

        return args

    def output(self):
        return [LocalTarget(join(self.output_dir, 'Aligned.{}.out'.format(self.output_format))),
                LocalTarget(join(self.output_dir, 'SJ.tab.out'))]
