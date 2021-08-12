from setuptools import setup, find_packages

setup(name='bioluigi',
      version='0.0.6',
      description='Reusable and maintained Luigi tasks to incorporate in bioinformatics pipelines',
      long_description='file: README.md',
      author='Guillaume Poirier-Morency',
      author_email='poirigui@msl.ubc.ca',
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'bioluigi = bioluigi.cli:main']},
      install_requires=['click', 'luigi', 'requests', 'babel'])
