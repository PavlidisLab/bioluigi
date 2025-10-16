from bioluigi.tasks import cellranger

def test_cellranger_count():
    task = cellranger.CellRangerCount()
    assert task.program_args() == ['cellranger', 'count', '']

def test_bamtofastq():
    task = cellranger.BamToFastq(input_file='test.bam', output_dir='outdir')
    assert task.program_args() == ['bamtofastq', '--nthreads', 1, 'test.bam', None]