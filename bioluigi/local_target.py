import os
import random
from contextlib import contextmanager
from os.path import splitext

import luigi

class LocalTarget(luigi.LocalTarget):
    """A *patched* LocalTarget that preserves file extensions in temporary filenames and removes the temporary path on error.

    A patch has been submitted for this in https://github.com/spotify/luigi/pull/3365
    """

    @contextmanager
    def temporary_path(self):
        num = random.randrange(0, 10_000_000_000)
        slashless_path, ext = splitext(self.path.rstrip('/').rstrip("\\"))
        _temp_path = '{}-luigi-tmp-{:010}{}{}'.format(
            slashless_path,
            num,
            ext,
            self._trailing_slash())
        # TODO: os.path doesn't make sense here as it's os-dependent
        tmp_dir = os.path.dirname(slashless_path)
        if tmp_dir:
            self.fs.mkdir(tmp_dir, parents=True, raise_if_exists=False)
        try:
            yield _temp_path
        except:
            if self.fs.exists(_temp_path):
                self.fs.remove(_temp_path)
            raise
        # We won't reach here if there was an user exception.
        self.fs.rename_dont_move(_temp_path, self.path)

    def __repr__(self):
        return f'LocalTarget(path={self.path}, format={self.format}, fs={self.fs})'