#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022, 2023 Pytroll developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

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

"""Level-1 processing for Direct Readout S-NPP/JPSS ATMS data.


Using the CSPP level-1 processor from the SSEC, Wisconsin, based on the ADL
software from NASA. Listen for pytroll messages of ready RDR files trigger
processing on direct readout RDR data (granules or full swaths).

"""

import argparse
import logging
import ast
import configparser
import os
import sys
import logging
import logging.handlers

from cspp_runner.atms_rdr2sdr_runner import AtmsSdrRunner
from cspp_runner.config import read_config
from cspp_runner.logger import setup_logging

CSPP_SDR_HOME = os.environ.get("CSPP_SDR_HOME", '')

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


LOG = logging.getLogger(__name__)


def get_parser():
    """Get parser for commandline-arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file",
                        required=True,
                        dest="config_file",
                        type=str,
                        default=None,
                        help="The file containing configuration parameters.")
    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")
    return parser


def parse_args():
    """Parse command-line arguments."""
    parser = get_parser()
    return parser.parse_args()


def main():
    """Start the CSPP ATMS runner."""
    cmd_args = parse_args()
    print("Read config from", cmd_args.config_file)

    setup_logging(cmd_args)

    #OPTIONS = read_config(cmd_args.config_file)
    #publish_topics = OPTIONS.get('publish_topics')
    #subscribe_topics = OPTIONS.get('subscribe_topics')
    #viirs_sdr_options = OPTIONS.get("atms_sdr_options")

    breakpoint()

    try:
        atms = AtmsSdrRunner(cmd_args.config_file)
    except Exception as err:
        LOG.error('ATMS RDR to SDR processing crashed: %s', str(err))
        sys.exit(1)
    try:
        atms.start()
        atms.join()
    except KeyboardInterrupt:
        LOG.debug("Interrupting")
    finally:
        atms.close()


if __name__ == "__main__":
    main()
