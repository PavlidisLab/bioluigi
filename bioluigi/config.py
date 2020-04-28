import luigi

class bioluigi(luigi.Config):
    scheduler = luigi.Parameter(default='local', significant=False, description='Default scheduler to use in ScheduledExternalProgram')
    scheduler_partition = luigi.OptionalParameter(default='', significant=False, description='Node partition to use for scheduling jobs if supported')
    scheduler_extra_args = luigi.ListParameter(default=[], significant=False, description='List of extra arguments to pass to the scheduler')
    prefetch_bin = luigi.Parameter(default='prefetch')
    fastqdump_bin = luigi.Parameter(default='fastq-dump')
    cutadapt_bin = luigi.Parameter(default='cutadapt')
    fastqc_bin = luigi.Parameter(default='fastqc')
    star_bin = luigi.Parameter(default='STAR')
    rsem_dir = luigi.Parameter(default='')
    bcftools_bin = luigi.Parameter(default='bcftools')
    vep_bin = luigi.Parameter(default='vep')
    vep_dir = luigi.OptionalParameter(default='', significant=False)
    multiqc_bin = luigi.Parameter(default='multiqc')
