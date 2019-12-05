Getting started with Bioluigi
=============================

The first step is to install Bioluigi package in your current environment with
pip.

.. code-block:: bash

   pip install git+https://github.com/PavlidisLab/bioluigi.git

Then, we need to create a ``luigi.cfg`` file to indicate the location of the
tools that will be invoked. You can omit this step if they are defined in your
``$PATH``.

.. code-block:: ini

   [bioluigi]
   cutadapt_bin=cutadapt
   star_bin=STAR
   rsem_dir=

Now, let's setup a simple single read RNA-Seq pipeline. For this use case, we
will first trim our single-end reads and then align them on a human reference
genome.

::

   import datetime
   import os

   import luigi
   from import bioluigi.tasks import cutadapt, rsem

   def QuantifySample(luigi.Task):
       sample_id = luigi.Parameter()

       def input(self):
           return luigi.LocalTarget('{}.fastq.gz'.format(self.sample_id))

       def run(self):
           sample = self.input(description='Sample identifier')

           trimmed_reads = yield cutadapt.TrimReads(sample.path,
                                                    adapter_3prime='ACGTAGCGAGA...')

           yield rsem.CalculateExpression('genomes/hg38_ensembl98/annotation.gtf',
                                          ['genomes/hg38_ensembl98/primary_assembly.fa'],
                                          [trimmed_reads.path],
                                          'references/hg38_ensembl98',
                                          self.sample_id,
                                          aligner='star',
                                          walltime=datetime.timedelta(hours=4),
                                          cpus=8,
                                          memory=32)

       def output(self):
           return luigi.LocalTarget('{}.genes.results'.format(self.sample_id))

The important features to notice here are ``walltime``, ``cpus`` and ``memory``
parameters. When run with a supporting :doc:`scheduler <schedulers>`, all the
tasks will be dispatched on the cluster with allocated resources.

To run our task on a given sample:

.. code-block::

   luigi --module tasks QuantifySample --sample-id SRR...
