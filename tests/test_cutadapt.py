from bioluigi.tasks import cutadapt

def test_trim_single_end():
    task = cutadapt.TrimReads(input_file='R1.fastq.gz', output_file='R1.fastq.gz', adapter_3prime='TCGA')
    assert task.program_args() == ['cutadapt', '-j', '1', '-a', 'TCGA', '-o', 'R1.fastq.gz', 'R1.fastq.gz']
    assert len(task.output()) == 1

def test_trim_paired_end():
    task = cutadapt.TrimPairedReads(input_file='R1.fastq.gz', input2_file='R2.fastq.gz', output_file='R1.fastq.gz',
                                    output2_file='R2.fastq.gz', adapter_3prime='TCGA', reverse_adapter_3prime='TCGA')
    assert task.program_args() == ['cutadapt', '-j', '1', '-a', 'TCGA', '-A', 'TCGA', '-o', 'R1.fastq.gz', '-p',
                                   'R2.fastq.gz', 'R1.fastq.gz', 'R2.fastq.gz']
    assert len(task.output()) == 2
