import luigi

class BioluigiConfig(luigi.Config):
    """Bioluigi configuration"""

    @classmethod
    def get_task_family(cls):
        return 'bioluigi'

    scheduler = luigi.Parameter(default='local',
                                description='Default scheduler to use in ScheduledExternalProgram')
    scheduler_partition = luigi.OptionalParameter(default=None,
                                                  description='Partition (or queue) to use for scheduling jobs if supported by the scheduler')
    scheduler_extra_args = luigi.ListParameter(default=[],
                                               description='List of extra arguments to pass to the scheduler')
    prefetch_bin = luigi.Parameter(default='prefetch')
    fastqdump_bin = luigi.Parameter(default='fastq-dump')
    cutadapt_bin = luigi.Parameter(default='cutadapt')
    fastqc_bin = luigi.Parameter(default='fastqc')
    star_bin = luigi.Parameter(default='STAR')
    rsem_dir = luigi.Parameter(default='')
    samtools_bin = luigi.Parameter(default='samtools')
    bcftools_bin = luigi.Parameter(default='bcftools')
    vep_bin = luigi.Parameter(default='vep')
    vep_dir = luigi.OptionalParameter(default=None)
    multiqc_bin = luigi.Parameter(default='multiqc')
    cellranger_bin = luigi.Parameter(default='cellranger')
    bamtofastq_bin = luigi.Parameter(default='bamtofastq')

# for backward compatibility, use Bioluig
bioluigi = BioluigiConfig
"""
Kept for backward-compatibility, use BioluigiConfig instead
"""
