from bioluigi.tasks import bcftools

def test_view():
    task = bcftools.View('input.vcf', 'output.vcf')
    assert '-i' not in task.program_args()
    assert '-e' not in task.program_args()
    assert '-f' not in task.program_args()
    assert '-R' not in task.program_args()

def test_merge():
    task = bcftools.Merge(['input1.vcf', 'input2.vcf'], 'output.vcf')
    assert 'input1.vcf' in task.program_args()
    assert 'input2.vcf' in task.program_args()
    assert 'output.vcf' in task.program_args()
