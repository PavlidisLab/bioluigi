from bioluigi.tasks import cellranger

def test_cellranger_count():
    task = cellranger.CellRangerCount(id='test', transcriptome_dir='test', fastqs_dir='test', output_dir='test')
    assert not task.complete()
    assert task.program_args() == ['cellranger', 'count', '--id', 'test', '--create-bam', 'false',
                                   '--transcriptome', 'test', '--fastqs', 'test', '--output-dir',
                                   'test',
                                   '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']

    task = cellranger.CellRangerCount(id='test', transcriptome_dir='test', fastqs_dir='test')
    assert task.program_args() == ['cellranger', 'count', '--id', 'test', '--create-bam', 'false',
                                   '--transcriptome', 'test', '--fastqs', 'test',
                                   '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']
    assert not task.complete()
    assert task.output().path == 'test'

    task = cellranger.CellRangerCount(id='test', transcriptome_dir='test', fastqs_dir='test', working_directory='/tmp')
    assert task.program_args() == ['cellranger', 'count', '--id', 'test', '--create-bam', 'false',
                                   '--transcriptome', 'test', '--fastqs', 'test',
                                   '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']
    task._tmp_output_dir = '/tmp/test-luigi-tmp-1901293012'
    assert task.program_args() == ['cellranger', 'count', '--id', 'test-luigi-tmp-1901293012', '--create-bam', 'false',
                                   '--transcriptome', 'test', '--fastqs', 'test',
                                   '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']
    assert task.output().path == '/tmp/test'

    # during run(), the output should be produced atomically in a temporary directory
    task = cellranger.CellRangerCount(id='test', transcriptome_dir='test', fastqs_dir='test', working_directory='/tmp')
    task._tmp_output_dir = None
    assert task.program_args() == ['cellranger', 'count', '--id', 'test', '--create-bam', 'false',
                                   '--transcriptome', 'test', '--fastqs', 'test',
                                   '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']
    task._tmp_output_dir = 'test-luigi-tmp-1901293012'
    assert task.program_args() == ['cellranger', 'count', '--id', 'test-luigi-tmp-1901293012', '--create-bam', 'false',
                                   '--transcriptome', 'test', '--fastqs', 'test',
                                   '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']
    assert task.output().path == '/tmp/test'

def test_cellranger_arc_count():
    task = cellranger.CellRangerArcCount(id='test', reference_dir='test', fastqs_dir='test', output_dir='test')
    assert task.program_args() == ['cellranger-arc', 'count', '--id', 'test', '--create-bam', 'false', '--output-dir',
                                   'test',
                                   '--reference', 'test', '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']

    task = cellranger.CellRangerArcCount(id='test', reference_dir='test', fastqs_dir='test')
    assert task.program_args() == ['cellranger-arc', 'count', '--id', 'test', '--create-bam', 'false',
                                   '--reference', 'test', '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']

    task = cellranger.CellRangerArcCount(id='test', reference_dir='test', fastqs_dir='test', working_directory='/tmp')
    assert task.program_args() == ['cellranger-arc', 'count', '--id', 'test', '--create-bam', 'false',
                                   '--reference', 'test', '--disable-ui', '--nosecondary',
                                   '--jobmode', 'local', '--localcores', '8', '--localmem', '64']

def test_bamtofastq():
    task = cellranger.BamToFastq(input_file='test.bam', output_dir='outdir')
    assert task.program_args() == ['bamtofastq', '--nthreads', '1', 'test.bam', 'outdir']
