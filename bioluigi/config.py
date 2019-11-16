import luigi

class bioluigi(luigi.Config):
    fastqc_bin = luigi.Parameter(default='fastqc')
    star_bin = luigi.Parameter(default='STAR')
    bcftools_bin = luigi.Parameter(default='bcftools')
    vep_bin = luigi.Parameter(default='vep')
    vep_dir = luigi.OptionalParameter(default='', significant=False)
