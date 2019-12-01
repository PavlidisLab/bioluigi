from setuptools import setup, find_packages

setup(name='bioluigi',
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'bioluigi = bioluigi.cli:main',
              'bioluigi-gen-workflow = bioluigi.cwl:main']},
      install_requires=['click', 'luigi'])
