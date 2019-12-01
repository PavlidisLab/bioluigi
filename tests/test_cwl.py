import luigi
from bioluigi.cwl import *
import yaml

class CountExperiment(luigi.Task):
    experiment_id = luigi.Parameter()
    source = luigi.Parameter()
    genome_build = luigi.Parameter()
    reference_build = luigi.Parameter()

def test_sp():
    return
    pkgs = gen_software_packages()
    assert any(pkg['package'] == 'sratoolkit' for pkg in pkgs)
    assert any(pkg['package'] == 'STAR' for pkg in pkgs)
    assert any(pkg['package'] == 'RSEM' for pkg in pkgs)

def test_gen_workflow():
    workflow = gen_workflow(CountExperiment('GSE75484', source='gemma', genome_build='mm10', reference_build='ncbi'))
    assert workflow['class'] == 'Workflow'
    assert 'cwlVersion' in workflow
    assert 'inputs' in workflow
    assert 'outputs' in workflow
    assert 'steps' in workflow
