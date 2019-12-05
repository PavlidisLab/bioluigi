from bioluigi.tasks import multiqc

def test_generate_report():
    task = multiqc.GenerateReport(['indir'], 'outdir')
    args = task.program_args()
    assert '--outdir' in args
    assert 'outdir' in args
    assert '--title' not in args
    assert '--comment' not in args
    assert args[-1] == 'indir'
