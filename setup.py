from setuptools import setup, find_packages

setup(name='bioluigi',
      version='0.0.2',
      description='Reusable and maintained Luigi tasks to incorporate in bioinformatics pipelines',
      long_description='file: README.md',
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'bioluigi = bioluigi.cli:main']},
      install_requires=['click', 'luigi', 'requests', 'babel'])
