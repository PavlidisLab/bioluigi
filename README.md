# Bioluigi

[![Python package](https://github.com/PavlidisLab/bioluigi/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/PavlidisLab/bioluigi/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/bioluigi/badge/?version=latest)](https://bioluigi.readthedocs.io/en/latest/?badge=latest)

Reusable and maintained Luigi tasks to incorporate in bioinformatics pipelines

## Features

Provides Luigi tasks for tools from [samtools](http://www.htslib.org/doc/samtools.html), [bcftools](http://www.htslib.org/doc/bcftools.html), [STAR](https://github.com/alexdobin/STAR), [RSEM](http://deweylab.github.io/RSEM/), [vcfanno](http://brentp.github.io/vcfanno/), [GATK](https://software.broadinstitute.org/gatk/), [Ensembl VEP](https://www.ensembl.org/vep) and much more!

Reuses as much as possible the `ExternalProgramTask` interface from the [external_program contrib module](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.external_program.html)
and extends its feature to make it work on modern scheduler such as [Slurm](https://slurm.schedmd.com/).

Provides basic resource management for a local scheduler: all tasks are annotated with reasonable default `cpus` and `memory` parameters that can be tuned and constrained via the `[resources]` configuration. In the case of externally scheduled tasks, the resource management is deferred.

Provides a command-line interface for interacting more conveniently with Luigi scheduler.

```bash
bioluigi list [--status STATUS] [--user USER] [--detailed] TASK_GLOB
bioluigi show TASK_ID
```

## Tools

Here's a list of supported tools:

 - [sratoolkit](https://ncbi.github.io/sra-tools/) with `prefetch` and `fastq-dump`
 - [bcftools](http://www.htslib.org/doc/bcftools.html)
 - [FastQC](https://www.bioinformatics.babraham.ac.uk/projects/fastqc/)
 - [MultiQC](https://multiqc.info/)

## Schedulers

 - local
 - [Slurm](https://slurm.schedmd.com/)

## Examples

The most convenient way of using the pre-defined tasks is to yield them dynamically in the body of the `run` function. It's also possible to require them since they inherit from `luigi.Task`.

```python
import luigi
from bioluigi.tasks import bcftools

def MyTask(luigi.Task)
    def input(self):
        return luigi.LocalTarget('source.vcf.gz')

    def run(self):
        yield bcftools.Annotate(self.input().path,
                                annotations_file,
                                self.output().path,
                                ...,
                                scheduler='slurm',
                                cpus=8)

    def output(self):
        return luigi.LocalTarget('annotated.vcf.gz')
```

You can define your own scheduled task by implementing the `ScheduledExternalProgramTask` class. Note that the default scheduler is `local` and will use Luigi's `[resources]` allocation mechanism.

```python
import datetime
from bioluigi.scheduled_external_program import ScheduledExternalProgramTask

class MyScheduledTask(ScheduledExternalProgramTask):
    scheduler = 'slurm'
    walltime = datetime.timedelta(seconds=10)
    cpus = 1
    memory = 1

    def program_args(self):
        return ['sleep', '10']
```
