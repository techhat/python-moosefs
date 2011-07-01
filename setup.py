#!/usr/bin/python2
'''
The setup script for python-moosefs
'''
import os
import sys
from distutils.core import setup

NAME = 'python-moosefs'
VER = '0.0.1'
DESC = 'Module to gather information from the MooseFS master'

setup(name=NAME,
      version=VER,
      description=DESC,
      author='Joseph Hall',
      author_email='perlhoser@gmail.com',
      url='https://github.com/techhat/python-moosefs',
      classifiers = [
          'Programming Language :: Python',
          ],
      py_modules=['moosefs',
                ],
     )
