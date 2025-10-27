from abc import ABC
from math import ceil
from os import makedirs
from os.path import join, basename
from typing import Optional

import luigi

from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

class BaseCellRangerCountTarget(LocalTarget):
    def __init__(self, output_dir):
        super().__init__(output_dir)

        # deprecated, simply use path
        self.output_dir = output_dir

        self.molecule_info = join(output_dir, 'outs/molecule_info.tsv')
        self.metrics_summary = join(output_dir, 'outs/metrics_summary.html')
        self.web_summary = join(output_dir, 'outs/web_summary.html')

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

class CellRangerCountTarget(BaseCellRangerCountTarget):
    def __init__(self, output_dir):
        super().__init__(output_dir)
        self.molecule_info = join(output_dir, 'outs/molecule_info.tsv')
        self.metrics_summary = join(output_dir, 'outs/metrics_summary.html')
        self.web_summary = join(output_dir, 'outs/web_summary.html')

class CellRangerCount(ScheduledExternalProgramTask):
    """
    Run the Cell Ranger count pipeline.

    Important note: Cell Ranger requires a CPU with AVX instruction sets (and eventually will require AVX2). If using a
    scheduler, you might want to constraint machines that can run this job by passing <scheduler>_extra_args. For Slurm,
     use the --constraint option.
    """
    task_namespace = 'cellranger'

    id: str = luigi.Parameter(description='ID for the run.')
    transcriptome_dir: str = luigi.Parameter(
        description='Directory containing a valid Cell Ranger transcriptome reference.')
    fastqs_dir: str = luigi.Parameter(description='Directory containing FASTQ files to analyze.')
    output_dir: Optional[str] = luigi.OptionalParameter(default=None,
                                                        description='Directory where to output the analysis results. '
                                                                    'This option was introduced in Cell Ranger 7.2. '
                                                                    'For workflow with previous versions, you can set working_directory '
                                                                    'and id instead, the output directory will be join(working_directory, id).')

    expect_cells: Optional[int] = luigi.OptionalIntParameter(default=None, positional=False)
    force_cells: Optional[int] = luigi.OptionalIntParameter(default=None, positional=False)
    chemistry: Optional[str] = luigi.OptionalParameter(default=None, positional=False,
                                                       description='Chemistry to use for Cell Ranger (default is to auto-detect).')

    # https://www.10xgenomics.com/support/software/cell-ranger/downloads/cr-system-requirements
    cpus = 8
    memory = 64

    # in the case of Cell Ranger, the working directory is a significant parameter because it affects the output
    # location
    working_directory: Optional[str] = luigi.OptionalParameter(default=None, significant=True, positional=False,
                                                               description='If output_dir is not set, Cell Ranger output will be stored relative to the working directory.')

    _tmp_output_dir: Optional[str] = None

    @property
    def resources(self):
        res = super().resources
        res['cellranger_count_jobs'] = 1
        return res

    def program_args(self):
        args = [cfg.cellranger_bin, 'count']
        args.extend([
            '--id', basename(self._tmp_output_dir) if self.output_dir is None and self._tmp_output_dir else self.id,
            '--create-bam', 'false',
            '--transcriptome', self.transcriptome_dir,
            '--fastqs', self.fastqs_dir])
        if self.output_dir:
            args.extend(['--output-dir', self._tmp_output_dir if self._tmp_output_dir else self.output_dir])
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
        with self.output().temporary_path() as self._tmp_output_dir:
            makedirs(self._tmp_output_dir)
            super().run()

    def output(self):
        if self.output_dir:
            return CellRangerCountTarget(self.output_dir)
        elif self.working_directory:
            return CellRangerCountTarget(join(self.working_directory, self.id))
        else:
            return CellRangerCountTarget(self.id)

class CellRangerArcCountTarget(BaseCellRangerCountTarget):
    def __init__(self, output_dir):
        super().__init__(output_dir)
        self.summary = join(output_dir, 'outs/summary.csv')
        self.web_summary = join(output_dir, 'outs/web_summary.html')
        self.gex_molecule_info = join(output_dir, 'outs/gex_molecule_info.h5')
        self.per_barcode_metrics = join(output_dir, 'outs/per_barcode_metrics.csv')
        self.atac_cut_sites = join(output_dir, 'outs/atac_cut_sites.bigwig')
        self.atac_fragments = (join(output_dir, 'outs/atac_fragments.tsv.gz'),
                               join(output_dir, 'outs/atac_fragments.tsv.gz.tbi'))
        self.atac_peak_annotation = join(output_dir, 'outs/atac_peak_annotation.tsv')
        self.atac_peaks = join(output_dir, 'outs/atac_peaks.bed')

class CellRangerArcCount(ScheduledExternalProgramTask):
    task_namespace = 'cellranger'

    id: str = luigi.Parameter()
    reference_dir: str = luigi.Parameter()
    fastqs_dirs = luigi.ListParameter()
    atac_dirs = luigi.ListParameter()

    output_dir: Optional[str] = luigi.OptionalParameter(default=None)

    # https://www.10xgenomics.com/support/software/cell-ranger-arc/downloads/system-requirements
    cpus = 8
    memory = 64

    # in the case of Cell Ranger, the working directory is a significant parameter because it affects the output
    # location
    working_directory: Optional[str] = luigi.OptionalParameter(default=None, significant=True, positional=False,
                                                               description='If output_dir is not set, Cell Ranger output will be stored relative to the working directory.')

    _libraries: str
    _tmp_output_dir: Optional[str] = None

    @property
    def resources(self):
        res = super().resources
        res['cellranger_count_jobs'] = 1
        return res

    def program_args(self):
        args = [cfg.cellranger_arc_bin, 'count',
                '--id', basename(self._tmp_output_dir) if self.output_dir and self._tmp_output_dir else self.id,
                '--reference', self.reference_dir,
                '--libraries', self._libraries if self._libraries else 'libraries.csv']
        if self.output_dir:
            args.extend(['--output-dir', self._tmp_output_dir if self._tmp_output_dir else self.output_dir])
        return args

    def run(self):
        with self.output().temporary_path() as self._tmp_output_dir:
            makedirs(self._tmp_output_dir)
            self._libraries = join(self._tmp_output_dir, 'libraries.csv')
            with open(self._libraries, 'w') as f:
                # TODO: generate libraries
                f.write('\n')
            super().run()

    def output(self):
        if self.output_dir:
            return CellRangerArcCountTarget(self.output_dir)
        elif self.working_directory:
            return CellRangerArcCountTarget(join(self.working_directory, self.id))
        else:
            return CellRangerArcCountTarget(self.id)

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
