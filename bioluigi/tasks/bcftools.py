import luigi
from luigi.contrib.external_program import ExternalProgramTask
from os.path import join

class AnnotateVcf(ExternalProgramTask):
    vcf_file = luigi.Parameter()
    annotated_vcf_file = luigi.Parameter()

    rename_chrs = luigi.OptionalParameter(positional=False, default='')

    # options given an annotation file
    annotations_vcf_file = luigi.OptionalParameter(positional=False, default='')
    columns = luigi.ListParameter(positional=False)

    def program_args(self):
        args = ['bcftools', 'annotate']

        if self.rename_chrs is not None:
            args.extend(['--rename-chrs', self.rename_chrs])

        if self.annotations_vcf_file:
            args.extend(['-a', self.annotations_vcf_file])
            args.extend(['-c', ','.join(self.columns)])

        args.extend([
            '-Oz',
            '-o', self.annotated_vcf_file,
            self.vcf_file])

        return args

    def output(self):
        return luigi.LocalTarget(self.annotated_vcf_file)

class SortVcf(ExternalProgramTask):
    vcf_file = luigi.Parameter()
    sorted_vcf_file = luigi.Parameter()

    tmp_dir = luigi.Parameter(default='/tmp', significant=False)

    def program_args(self):
        return ['bcftools', 'sort', self.vcf_file,
                '-Oz',
                '--output-file', self.output().path,
                '--temp-dir', self.tmp_dir]

    def output(self):
        return luigi.LocalTarget(self.sorted_vcf_file)


class IndexVcf(ExternalProgramTask):
    """
    Use tabix to create a tabular index for a VCF.
    """
    vcf_file = luigi.Parameter()

    def program_args(self):
        return ['tabix', '-p', 'vcf', self.vcf_file]

    def output(self):
        return luigi.LocalTarget(self.vcf_file + '.tbi')

class ApplyFilterVcf(ExternalProgramTask):
    """Apply the filter described in the VCF."""
    vcf_file = luigi.Parameter()
    filtered_vcf_file = luigi.Parameter()
    regions_file = luigi.Parameter()

    def program_args(self):
        args = ['bcftools', 'view']

        if self.regions_file is not None:
            args.extend(['--regions-file', self.regions_file])

        args.extend([
            '-f', 'PASS',
            '-Oz',
            '-o', self.output().path])

        args.append(self.vcf_file)

        return args

    def output(self):
        return luigi.LocalTarget(self.filtered_vcf_file)

class IntersectVcf(ExternalProgramTask):
    vcf_file = luigi.Parameter()
    vcf_file2 = luigi.Parameter()
    output_dir = luigi.Parameter()

    def program_args(self):
        return ['bcftools', 'isec', '-p', self.output_dir, self.vcf_file, self.vcf_file2]

    def output(self):
        return [luigi.LocalTarget(join(self.output_dir, '000{}.vcf'.format(i))) for i in range(4)]
