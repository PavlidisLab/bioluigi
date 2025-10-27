# Bioluigi

[![Python package](https://github.com/PavlidisLab/bioluigi/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/PavlidisLab/bioluigi/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/bioluigi/badge/?version=latest)](https://bioluigi.readthedocs.io/en/latest/?badge=latest)

Reusable and maintained Luigi tasks to incorporate in bioinformatics pipelines

## Features

Provides Luigi tasks for various bioinformatics tools and much more!

Provide `ScheduledExternalProgramTask` as an alternative to `ExternalProgramTask` from
the [external_program contrib module][^1]
to make it work on modern scheduler such as [Slurm][^2] or locally with Luigi resources.

Provides basic resource management for a local scheduler: all tasks are annotated with reasonable default `cpus` and
`memory` parameters that can be tuned and constrained via the `[resources]` configuration. In the case of externally
scheduled tasks, the resource management is deferred.

Provides a command-line interface for interacting more conveniently with Luigi scheduler.

```bash
bioluigi list [--status STATUS] [--user USER] [--detailed] TASK_GLOB
bioluigi show TASK_ID
```

## Supported tools

Here's a list of supported tools:

- [sratoolkit](https://ncbi.github.io/sra-tools/) with `prefetch` and `fastq-dump`
- [bcftools](http://www.htslib.org/doc/bcftools.html)
- [FastQC](https://www.bioinformatics.babraham.ac.uk/projects/fastqc/)
- [MultiQC](https://multiqc.info/)
- [RSEM](https://deweylab.github.io/RSEM/)
- [STAR](https://github.com/alexdobin/STAR)
- [Cell Ranger](https://www.10xgenomics.com/support/software/cell-ranger/latest)
- [Ensembl VEP](https://www.ensembl.org/vep)

## Schedulers

- local
- [Slurm](https://slurm.schedmd.com/)

## Usage

You must set up a `luigi.cfg` configuration with some basic stuff such as resources, etc. Refer
to [examples.luigi.cfg](example.luigi.cfg) for some examples.

The most convenient way of using the pre-defined tasks is to yield them dynamically in the body of the `run` function.
It's also possible to require them since they inherit from `luigi.Task`.

```python
import luigi
from luigi.util import requires

from bioluigi.tasks import bcftools

class ProduceAnnotations(luigi.ExternalTask):
  def output(self):
    return luigi.LocalTarget('annotations.vcf')

@requires(ProduceAnnotations)
class AnnotateFile(luigi.Task):
  input_file = luigi.Parameter()

  def run(self):
    yield bcftools.Annotate(input_file=self.input_file,
                            annotation_file=self.input().path,
                            output_file=self.output().path,
                            scheduler='slurm',
                            cpus=8)

  def output(self):
    return luigi.LocalTarget('annotated.vcf.gz')
```

You can define your own scheduled task by implementing the `ScheduledExternalProgramTask` class. Note that the default
scheduler is `local` and will use Luigi's `[resources]` allocation mechanism.

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

## Examples

- [Pavlidis Lab RNA-Seq Pipeline](https://github.com/PavlidisLab/rnaseq-pipeline)

[^1]: https://luigi.readthedocs.io/en/stable/api/luigi.contrib.external_program.html

[^2]: https://slurm.schedmd.com/
