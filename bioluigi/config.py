import luigi

class bioluigi(luigi.Config):
    star_bin = luigi.Parameter(default='STAR')
    bcftools_bin = luigi.Parameter(default='bcftools')
    vep_bin = luigi.Parameter(default='vep')
    vep_cache_dir = luigi.OptionalParameter(default='')
