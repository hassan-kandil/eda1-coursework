#!/usr/bin/env python

from distutils.core import setup


# Helper function to read the requirements.txt
def parse_requirements(filename):
    with open(filename, 'r') as f:
        return f.read().splitlines()

setup(name='merizo_pipeline',
      version='1.1',
      description='Python code for EDA 1 coursework merizo search pipeline',
      url='TBC',
      packages=['merizo_pipeline'],
      install_requires=parse_requirements('requirements.txt'),
      entry_points={'console_scripts': ['build-index = dataeng.gather:parse_index_entry',
                                        'analyse = dataeng.analysis:analysis_entry',
                                        'combine = dataeng.combine:combine_entry'
                                        ]}
     )
