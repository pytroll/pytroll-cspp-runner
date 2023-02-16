#!/usr/bin/env python

# Copyright (c) 2013 - 2021 cspp-runner developers

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

"""Pytroll processing converting VIIRS RDR to SDR using CSPP.

Level-1 processing for VIIRS Suomi NPP Direct Readout data. Using the CSPP
level-1 processor from the SSEC, Wisconsin, based on the ADL from the NASA DRL.
Listen for pytroll messages from trollstalker, trollmoves, or other sources.
Trigger processing on direct readout RDR data (granules or full swaths).
"""


import argparse
import ast
import configparser
import os
import sys
import logging
import logging.handlers

from cspp_runner.runner import npp_rolling_runner

CSPP_SDR_HOME = os.environ.get("CSPP_SDR_HOME", '')
CSPP_RT_SDR_LUTS = os.path.join(CSPP_SDR_HOME, 'anc/cache/incoming_luts')

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


LOG = logging.getLogger(__name__)


def get_parser():
    """Get parser for commandline-arguments."""
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config-file",
                        required=True,
                        dest="config_file",
                        type=str,
                        default=None,
                        help="The file containing configuration parameters.")
    parser.add_argument("-C", "--config-section",
                        required=True,
                        help="log section to use")
    parser.add_argument("-l", "--log-file", dest="log",
                        type=str,
                        default=None,
                        help="The file to log to (stdout per default).")
    parser.add_argument("-p", "--publisher", type=str,
                        help="File with publisher config (YAML).")
    return parser


def parse_args():
    """Parse command-line arguments."""
    parser = get_parser()

    return parser.parse_args()


def main():
    """Start the CSPP runner."""

    CONF = configparser.ConfigParser()

    args = parse_args()
    print("Read config from", args.config_file)

    CONF.read(args.config_file)

    OPTIONS = {}
    for option, value in CONF.items(args.config_section, raw=True):
        OPTIONS[option] = value

    publish_topic = OPTIONS.get('publish_topic')
    subscribe_topics = OPTIONS.get('subscribe_topics').split(',')
    subscribe_topics = [topic for topic in subscribe_topics if topic]

    site = OPTIONS.get('site')

    thr_lut_files_age_days = OPTIONS.get('threshold_lut_files_age_days', 14)
    url_jpss_remote_lut_dir = OPTIONS['url_jpss_remote_lut_dir']
    url_jpss_remote_anc_dir = OPTIONS['url_jpss_remote_anc_dir']
    lut_dir = OPTIONS.get('lut_dir', CSPP_RT_SDR_LUTS)
    lut_update_stampfile_prefix = OPTIONS['lut_update_stampfile_prefix']
    anc_update_stampfile_prefix = OPTIONS['anc_update_stampfile_prefix']
    url_download_trial_frequency_hours = OPTIONS[
        'url_download_trial_frequency_hours']
    viirs_sdr_call = OPTIONS["viirs_sdr_call"]
    viirs_sdr_options = ast.literal_eval(OPTIONS["viirs_sdr_options"])

    if args.log is not None:
        ndays = int(OPTIONS.get("log_rotation_days", 1))
        ncount = int(OPTIONS.get("log_rotation_backup", 7))
        handler = logging.handlers.TimedRotatingFileHandler(
                args.log, when='midnight', interval=ndays,
                backupCount=ncount, encoding=None,
                delay=False, utc=True)

        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    npp_rolling_runner(
            thr_lut_files_age_days,
            url_download_trial_frequency_hours,
            lut_update_stampfile_prefix,
            lut_dir,
            url_jpss_remote_lut_dir,
            OPTIONS["mirror_jpss_luts"],
            url_jpss_remote_anc_dir,
            anc_update_stampfile_prefix,
            OPTIONS["mirror_jpss_ancillary"],
            subscribe_topics,
            site,
            OPTIONS["mode"],
            publish_topic,
            OPTIONS["level1_home"],
            viirs_sdr_call,
            viirs_sdr_options,
            int(OPTIONS.get("granule_time_tolerance", 10)),
            int(OPTIONS.get("ncpus", 1)),
            publisher_config=args.publisher,
            )


if __name__ == "__main__":
    main()
