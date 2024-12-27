#!/usr/bin/env python

from distutils.core import setup


# Helper function to read the requirements.txt
def parse_requirements(filename):
    with open(filename, 'r') as f:
        return f.read().splitlines()

setup(name='merizo_analysis',
      version='1.0',
      description='Python code for EDA 1 coursework merizo search pipeline',
      url='TBC',
      packages=['merizo_analysis']
     )
