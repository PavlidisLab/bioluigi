from bioluigi.tasks import sratoolkit

def test_prefetch():
    prefetch_task = sratoolkit.Prefetch('srr_accession', 'outdir')
    args = prefetch_task.program_args()
    assert 'cpus' in prefetch_task.resources
    assert 'prefetch_jobs' in prefetch_task.resources
    assert prefetch_task.resources['prefetch_jobs'] == 1

def test_fastq_dump():
    fastq_dump_task = sratoolkit.FastqDump('srr_accession', 'outdir', minimum_read_length=18)
    args = fastq_dump_task.program_args()
    assert '-M' in args
    assert 18 in args
    assert args[-3] == '--outdir'
    assert args[-2] == 'outdir'
    assert args[-1] == 'srr_accession'
    assert 'fastq_dump_jobs' in fastq_dump_task.resources
    assert fastq_dump_task.resources['fastq_dump_jobs'] == 1
