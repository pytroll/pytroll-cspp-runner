#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 - 2022 pytroll-cspp-runner developers

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Setup for cspp-runner.
"""

from setuptools import setup

try:
    # HACK: https://github.com/pypa/setuptools_scm/issues/190#issuecomment-351181286
    # Stop setuptools_scm from including all repository files
    import setuptools_scm.integration
    setuptools_scm.integration.find_files = lambda _: []
except ImportError:
    pass

DESCRIPTION = "Pytroll runner for CSPP"

try:
    with open('./README.md', 'r') as fd:
        long_description = fd.read()
except IOError:
    long_description = ''


NAME = 'cspp_runner'

setup(name=NAME,
      description=DESCRIPTION,
      author='Adam Dybroe',
      author_email='adam.dybroe@smhi.se',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/pytroll-cspp-runner",
      long_description=long_description,
      packages=['cspp_runner', ],
      data_files=[],
      install_requires=['posttroll>1.7', 'trollsift'],
      # test_requires=['mock'],
      # test_suite='cspp_runner.tests.suite',
      python_requires='>=3.8',
      zip_safe=False,
      use_scm_version=True
      )
