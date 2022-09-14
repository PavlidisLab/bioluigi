from setuptools import setup, find_packages
import sys

# Luigi 3+ does not support Python 2
if sys.version_info.major == 2:
    luigi_dep = 'luigi~=2.8'
else:
    luigi_dep = 'luigi'

setup(name='bioluigi',
      version='0.0.12',
      description='Reusable and maintained Luigi tasks to incorporate in bioinformatics pipelines',
      long_description='file: README.md',
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
          install_requires=['click', luigi_dep, 'requests', 'babel'])
