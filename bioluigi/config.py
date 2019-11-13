import luigi

class bioluigi(luigi.Config):
    star_bin = luigi.Parameter(default='STAR')
    bcftools_bin = luigi.Parameter(default='bcftools')
    tabix_bin = luigi.Parameter(default='tabix')
    vep_bin = luigi.Parameter(default='vep')
    vep_cache_dir = luigi.OptionalParameter(default='')
