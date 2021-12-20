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

"""Level-1 processing for VIIRS Suomi NPP Direct Readout data. Using the CSPP
level-1 processor from the SSEC, Wisconsin, based on the ADL from the NASA DRL.
Listen for pytroll messages from Nimbus (NPP file dispatch) and trigger
processing on direct readout RDR data (granules or full swaths)
"""


import argparse
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


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file",
                        required=True,
                        dest="config_file",
                        type=str,
                        default=None,
                        help="The file containing configuration parameters.")
    parser.add_argument("-C", "--config-section",
                        default=os.getenv("SMHI_MODE") or "dev",
                        help="log section to use")
    parser.add_argument("-l", "--log-file", dest="log",
                        type=str,
                        default=None,
                        help="The file to log to (stdout per default).")

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

    PUBLISH_TOPIC = OPTIONS.get('publish_topic')
    SUBSCRIBE_TOPICS = OPTIONS.get('subscribe_topics').split(',')
    for item in SUBSCRIBE_TOPICS:
        if len(item) == 0:
            SUBSCRIBE_TOPICS.remove(item)

    SITE = OPTIONS.get('site')

    thr_lut_files_age_days = OPTIONS.get('threshold_lut_files_age_days', 14)
    url_jpss_remote_lut_dir = OPTIONS['url_jpss_remote_lut_dir']
    url_jpss_remote_anc_dir = OPTIONS['url_jpss_remote_anc_dir']
    lut_dir = OPTIONS.get('lut_dir', CSPP_RT_SDR_LUTS)
    lut_update_stampfile_prefix = OPTIONS['lut_update_stampfile_prefix']
    anc_update_stampfile_prefix = OPTIONS['anc_update_stampfile_prefix']
    url_download_trial_frequency_hours = OPTIONS[
        'url_download_trial_frequency_hours']

    if args.log is not None:
        ndays = int(OPTIONS.get("log_rotation_days", 1))
        ncount = int(OPTIONS.get("log_rotation_backup", 7))
        handler = handlers.TimedRotatingFileHandler(args.log,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

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

    LOG = logging.getLogger('viirs_dr_runner')

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
            OPTIONS["subscribe_topics"],
            OPTIONS["site"],
            OPTIONS["publish_topic"],
            OPTIONS["level1_home"],
            int(OPTIONS.get("ncpus", 1))
            )


if __name__ == "__main__":
    main()
