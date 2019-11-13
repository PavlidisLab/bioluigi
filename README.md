# Bioluigi

Reusable and maintained Luigi tasks to incorporate in bioinformatics pipelines

## Features

Provides Luigi tasks for tools from [samtools](http://www.htslib.org/doc/samtools.html), [bcftools](http://www.htslib.org/doc/bcftools.html), STAR, [RSEM](http://deweylab.github.io/RSEM/), [vcfanno](http://brentp.github.io/vcfanno/), [GATK](https://software.broadinstitute.org/gatk/), [Ensembl VEP](https://www.ensembl.org/vep) and much more!

Reuses as much as possible the `ExternalProgramTask` interface from the [external_program contrib module](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.external_program.html)
and extends its feature to make it work on modern scheduler such as [Slurm](https://slurm.schedmd.com/).

Provides basic resource management for a local scheduler: all tasks are annotated with reasonable default `cpus` and `memory` parameters that can be tuned and constrained via the `[resources]` configuration. In the case of externally scheduled tasks, the resource management is deferred.
