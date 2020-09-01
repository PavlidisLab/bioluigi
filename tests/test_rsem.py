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
    assert task.program_args()[-1] == 'references/hg38_ensembl98'

def test_calculate_expression():
    task = rsem.CalculateExpression('genomes/hg38_ensembl98/annotation.gtf',
                                    ['genomes/hg38_ensembl98/primary_assembly.fa'],
                                    'references/hg38_ensembl98',
                                    ['trimmed_reads.fastq.gz'],
                                    'sample_1',
                                    aligner='star',
                                    walltime=datetime.timedelta(hours=4),
                                    cpus=8,
                                    memory=32)

    assert task.sample_name == 'sample_1'
    assert '--star' in task.program_args()
    assert '--star-path' not in task.program_args()
    assert '--paired-end' not in task.program_args()

def test_calculate_expression_with_paired_reads():
    task = rsem.CalculateExpression('genomes/hg38_ensembl98/annotation.gtf',
                                    ['genomes/hg38_ensembl98/primary_assembly.fa'],
                                    'references/hg38_ensembl98',
                                    ['trimmed_reads_R1.fastq.gz', 'trimmed_reads_R2.fastq.gz'],
                                    'sample_1',
                                    aligner='star',
                                    walltime=datetime.timedelta(hours=4),
                                    cpus=8,
                                    memory=32)

    assert task.sample_name == 'sample_1'
    assert '--star' in task.program_args()
    assert '--star-path' not in task.program_args()
    assert '--paired-end' in task.program_args()
