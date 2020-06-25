import luigi
from os.path import join

from ..scheduled_external_program import ScheduledExternalProgramTask
from ..config import bioluigi

cfg = bioluigi()

class BcftoolsTask(ScheduledExternalProgramTask):
    task_namespace = 'bcftools'

    input_file = luigi.Parameter()

    include = luigi.OptionalParameter(positional=False, default=None)
    exclude = luigi.OptionalParameter(positional=False, default=None)
    regions = luigi.ListParameter(default=[], positional=False)
    regions_file = luigi.OptionalParameter(positional=False, default=None)
    samples = luigi.ListParameter(default=[], positional=False)
    samples_file = luigi.OptionalParameter(default=None, positional=False)
    apply_filters = luigi.OptionalParameter(positional=False, default=None)

    # FIXME: the '--threads' flag does not seem to work

    def subcommand_args(self):
        """Returns specific sub-command arguments."""
        raise NotImplementedError

    def subcommand_input_args(self):
        """
        Returns arguments to be appended at the input file location.

        This is meant to be to to deal with commands that accept multiple input
        files.
        """
        return [self.input_file]

    def program_args(self):
        args = [cfg.bcftools_bin]

        args.extend(self.subcommand_args())

        if self.include is not None:
            args.extend(['-i', self.include])

        if self.exclude is not None:
            args.extend(['-e', self.exclude])

        if self.regions:
            args.extend(['-r', ','.join(self.regions)])

        if self.regions_file is not None:
            args.extend(['-R', self.regions_file])

        if self.samples:
            args.extend(['-s', ','.join(self.samples)])

        if self.samples_file:
            args.extend(['-S', self.samples_file])

        if self.apply_filters is not None:
            args.extend(['-f', self.apply_filters])

        args.extend(self.subcommand_input_args())

        return args

class View(BcftoolsTask):
    """
    View a VCF and apply transformations and filters defined in :class:`BcftoolsTask`.
    """
    output_file = luigi.Parameter()
    output_format = luigi.Parameter(positional=False, default='z')

    def subcommand_args(self):
        return ['view',
            '--output-type', self.output_format,
            '--output-file', self.output_file]

    def output(self):
        return luigi.LocalTarget(self.output_file)

class Annotate(BcftoolsTask):
    """
    Annotate a VCF using bcftools annotate.
    """
    output_file = luigi.Parameter()
    output_format = luigi.Parameter(positional=False, default='z')

    # options given an annotation file
    annotations_file = luigi.OptionalParameter(positional=False, default=None)

    columns = luigi.ListParameter(positional=False, default=[])

    rename_chrs = luigi.OptionalParameter(positional=False, default=None)

    def subcommand_args(self):
        args = ['annotate']

        if self.rename_chrs is not None:
            args.extend(['--rename-chrs', self.rename_chrs])

        if self.annotations_file:
            args.extend(['-a', self.annotations_file])
            args.extend(['-c', ','.join(self.columns)])

        args.extend([
            '--output-type', self.output_format,
            '--output', self.output_file])

        return args

    def output(self):
        return luigi.LocalTarget(self.output_file)

class Sort(BcftoolsTask):
    output_file = luigi.Parameter()
    output_format = luigi.Parameter(positional=False, default='z')

    tmp_dir = luigi.Parameter(default='/tmp', significant=False)

    def subcommand_args(self):
        return ['sort',
                '--temp-dir', self.tmp_dir,
                '--output-type', self.output_format,
                '--output', self.output_file]

    def output(self):
        return luigi.LocalTarget(self.output_file)

class Index(BcftoolsTask):
    """
    Use tabix to create a tabular index for a VCF.
    """
    def subcommand_args(self):
        return ['index', '--tbi']

    def output(self):
        return luigi.LocalTarget(self.input_file + '.tbi')

class Intersect(BcftoolsTask):
    input_file2 = luigi.Parameter()
    output_dir = luigi.Parameter()

    def subcommand_args(self):
        return ['isec', '-p', self.output_dir]

    def subcommand_input_args(self):
        return [self.input_file, self.input_file2]

    def output(self):
        return [luigi.LocalTarget(join(self.output_dir, '000{}.vcf.gz'.format(i))) for i in range(4)]

class Merge(BcftoolsTask):
    """
    Merge the samples of two or more VCF files
    """
    input_file = luigi.ListParameter()

    filter_logic = luigi.ChoiceParameter(default='+', choices=['x', '+'], positional=False)
    info_rules = luigi.ListParameter(default=[], positional=False)

    output_file = luigi.Parameter()
    output_format = luigi.Parameter(positional=False, default='z')

    def subcommand_args(self):
        args = ['merge']

        args.extend(['--filter-logic', self.filter_logic])

        if self.info_rules:
            args.extend(['--info-rules', ','.join(self.info_rules)])

        args.extend([
            '--output-type', self.output_format,
            '--output', self.output_file])

        return args

    def subcommand_input_args(self):
        return self.input_file

    def output(self):
        return luigi.LocalTarget(self.output_file)
