from math import ceil
from os import makedirs
from os.path import join
from typing import Optional

import luigi

from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

class CellRangerCountTarget(luigi.Target):
    def __init__(self, output_dir):
        self.output_dir = output_dir

        # MEX
        self.filtered_mex = (join(output_dir, 'outs/filtered_feature_bc_matrix/barcodes.tsv.gz'),
                             join(output_dir, 'outs/filtered_feature_bc_matrix/features.tsv.gz'),
                             join(output_dir, 'outs/filtered_feature_bc_matrix/matrix.mtx.gz'))

        self.raw_mex = (join(output_dir, 'outs/raw_feature_bc_matrix/barcodes.tsv.gz'),
                        join(output_dir, 'outs/raw_feature_bc_matrix/features.tsv.gz'),
                        join(output_dir, 'outs/raw_feature_bc_matrix/matrix.mtx.gz'))

        # H5
        self.filtered_h5 = join(output_dir, 'outs/filtered_feature_bc_matrix.h5')
        self.raw_h5 = join(output_dir, 'outs/raw_feature_bc_matrix.h5')

        targets = []
        targets.extend(LocalTarget(f) for f in self.filtered_mex)
        targets.extend(LocalTarget(f) for f in self.raw_mex)
        targets.append(LocalTarget(self.filtered_h5))
        targets.append(LocalTarget(self.raw_h5))
        self._targets = targets

    def exists(self):
        return all(t.exists() for t in self._targets)

class CellRangerCount(ScheduledExternalProgramTask):
    """
    Run the Cell Ranger count pipeline.

    Important note: Cell Ranger requires a CPU with AVX instruction sets (and eventually will require AVX2). If using a
    scheduler, you might want to constraint machines that can run this job by passing <scheduler>_extra_args. For Slurm,
     use the --constraint option.
    """
    task_namespace = 'cellranger'

    id: str = luigi.Parameter()
    transcriptome_dir: str = luigi.Parameter(
        description='Directory containing a valid Cell Ranger transcriptome reference.')
    fastqs_dir: str = luigi.Parameter(description='Directory containing FASTQs to analyze.')
    output_dir: str = luigi.Parameter(description='Directory where to output the analysis results.')

    expect_cells: Optional[int] = luigi.OptionalIntParameter(default=None, positional=False)
    force_cells: Optional[int] = luigi.OptionalIntParameter(default=None, positional=False)
    chemistry: Optional[str] = luigi.OptionalParameter(default=None, positional=False,
                                                       description='Chemistry to use for Cell Ranger (default is to auto-detect).')

    # https://www.10xgenomics.com/support/software/cell-ranger/downloads/cr-system-requirements
    cpus = 8
    memory = 64

    _tmp_output_dir: Optional[str] = None

    @property
    def resources(self):
        res = super().resources
        res['cellranger_count_jobs'] = 1
        return res

    def program_args(self):
        args = [cfg.cellranger_bin, 'count']
        args.extend([
            '--id', self.id,
            '--create-bam', 'false',
            '--output-dir', self._tmp_output_dir if self._tmp_output_dir else self.output_dir,
            '--transcriptome', self.transcriptome_dir,
            '--fastqs', self.fastqs_dir])
        # TODO: consider making the UI available, I think we can link to it in Luigi
        args.append('--disable-ui')
        # save some time by omitting secondary analyses, could be removed if we want them
        args.append('--nosecondary')
        if self.expect_cells is not None:
            args.extend(['--expect-cells', str(self.expect_cells)])
        if self.force_cells is not None:
            args.extend(['--force-cells', str(self.force_cells)])
        if self.chemistry:
            args.extend(['--chemistry', self.chemistry])
        # TODO: consider re-using slurm
        args.extend([
            '--jobmode', 'local',
            '--localcores', str(self.cpus),
            '--localmem', str(int(ceil(self.memory)))])
        return args

    def run(self):
        with LocalTarget(self.output_dir).temporary_path() as self._tmp_output_dir:
            makedirs(self._tmp_output_dir)
            super().run()

    def output(self):
        return CellRangerCountTarget(self.output_dir)

class BamToFastq(ScheduledExternalProgramTask):
    """Uses bamtofastq from Cell Ranger to extract FASTQs from a 10x BAM file."""
    task_namespace = 'cellranger'

    input_file: str = luigi.Parameter()
    output_dir: str = luigi.Parameter()

    _tmp_output_dir: Optional[str] = None

    @property
    def resources(self):
        r = super().resources
        r.update({'cellranger_bamtofastq_jobs': 1, 'io_jobs': 1})
        return r

    def run(self):
        with self.output().temporary_path() as self._tmp_output_dir:
            super().run()

    def program_args(self):
        return [cfg.bamtofastq_bin, '--nthreads', str(self.cpus), self.input_file,
                self._tmp_output_dir if self._tmp_output_dir else self.output_dir]

    def output(self):
        return LocalTarget(self.output_dir)
