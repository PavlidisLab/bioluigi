from setuptools import setup, find_packages
import sys
from os.path import join, dirname

with open(join(dirname(__file__), 'README.md')) as f:
    long_description = f.read()

setup(name='bioluigi',
      version='0.2.1',
      description='Reusable and maintained Luigi tasks to incorporate in bioinformatics pipelines',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/pavlidisLab/bioluigi',
      license='Apache-2.0',
      license_files=('LICENSE',),
      author='Guillaume Poirier-Morency',
      author_email='poirigui@msl.ubc.ca',
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'bioluigi = bioluigi.cli:main']},
      install_requires=['click', 'luigi', 'python-daemon<3.0.0', 'requests'])
