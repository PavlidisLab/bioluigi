import gzip
import os
import tempfile
from os.path import join

from bioluigi.local_target import LocalTarget

tmp_dir = tempfile.mkdtemp()

def test_local_target_preserves_extension():
    target = LocalTarget(join(tmp_dir, 'A.fastq.gz'))
    with target.temporary_path() as path:
        assert path.endswith('.gz')
        with gzip.open(path, 'wt') as f:
            f.write('Hello World!')
    assert target.exists()

def test_local_target_on_error():
    target = LocalTarget(join(tmp_dir, 'test'))
    try:
        with target.temporary_path() as tmp_file:
            with open(tmp_file, 'w') as f:
                f.write('Hello world!')
            raise RuntimeError('Failed!')
    except RuntimeError:
        pass
    assert not target.exists()
    assert not os.path.exists(tmp_file)
