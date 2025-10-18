from bioluigi.tasks import cellranger

def test_cellranger_count():
    task = cellranger.CellRangerCount(id='test', transcriptome_dir='test', fastqs_dir='test', output_dir='test')
    assert task.program_args() == ['cellranger', 'count', '--id', 'test', '--create-bam', 'false', '--output-dir', 'test',
                                   '--transcriptome', 'test', '--fastqs', 'test', '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']

def test_bamtofastq():
    task = cellranger.BamToFastq(input_file='test.bam', output_dir='outdir')
    assert task.program_args() == ['bamtofastq', '--nthreads', '1', 'test.bam', 'outdir']