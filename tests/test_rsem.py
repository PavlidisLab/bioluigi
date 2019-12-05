import datetime

from bioluigi.tasks import rsem

def test_prepare_reference():
    task = rsem.PrepareReference('genomes/hg38_ensembl98/annotation.gtf',
                                 ['genomes/hg38_ensembl98/primary_assembly.fa'],
                                 'references/hg38_ensembl98',
                                 aligner='star',
                                 walltime=datetime.timedelta(hours=4),
                                 cpus=8,
                                 memory=32)

    assert task.reference_name == 'references/hg38_ensembl98'
    assert '--star' in task.program_args()
    assert '--star-path' not in task.program_args()

def test_calculate_expression():
    task = rsem.CalculateExpression('genomes/hg38_ensembl98/annotation.gtf',
                                    ['genomes/hg38_ensembl98/primary_assembly.fa'],
                                    ['trimmed_reads.fastq.gz'],
                                    'references/hg38_ensembl98',
                                    'sample_1',
                                    aligner='star',
                                    walltime=datetime.timedelta(hours=4),
                                    cpus=8,
                                    memory=32)

    assert task.sample_name == 'sample_1'
    assert '--star' in task.program_args()
    assert '--star-path' not in task.program_args()
