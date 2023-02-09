#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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

"""Fixtures for unittests."""

import pytest
from posttroll.message import Message
import stat


TEST_YAML_CONFIG_CONTENT = """# Location to store Sensor Data Record (SDR) files after CSPP SDR processing
# is completed.
level1_home: /path/to/where/the/atms/sdr/files/will/be/stored

working_dir: /san1/cspp/work

# CSPP-atms batch script and parameters:
atms_sdr_call: atms_sdr.sh

# Options to pass to the viirs_sdr_call
# see viirs_sdr.sh --help for explanation
atms_sdr_options:
  - '-a'
  - '-d'

# Topic to use for publishing posttroll messages
publish_topics:
  - /file/atms/sdr

# Posttroll topics to listen to (comma separated)
subscribe_topics:
  - /file/atms/rdr
"""

TEST_ATMS_COLLECTION_MESSAGE = """pytroll://atms/rdr/0/gatherer collection safusr.u@lxserv1043.smhi.se 2023-02-08T12:06:01.560943 v1.01 application/json {"start_time": "2023-02-08T11:54:17.200000", "end_time": "2023-02-08T12:04:57.100000", "orbit_number": 27071, "platform_name": "NOAA-20", "sensor": "atms", "format": "RDR", "type": "HDF5", "data_processing_level": "0", "variant": "DR", "collection_area_id": "euron1", "collection": [{"start_time": "2023-02-08T11:54:17.200000", "end_time": "2023-02-08T11:54:49.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1154172_e1154492_b00001_c20230208115457829000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1154172_e1154492_b00001_c20230208115457829000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:54:49.200000", "end_time": "2023-02-08T11:55:21.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1154492_e1155212_b00001_c20230208115538023000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1154492_e1155212_b00001_c20230208115538023000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:55:21.200000", "end_time": "2023-02-08T11:55:53.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1155212_e1155532_b00001_c20230208115558220000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1155212_e1155532_b00001_c20230208115558220000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:55:53.200000", "end_time": "2023-02-08T11:56:25.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1155532_e1156252_b00001_c20230208115638170000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1155532_e1156252_b00001_c20230208115638170000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:56:25.200000", "end_time": "2023-02-08T11:56:57.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1156252_e1156572_b00001_c20230208115718069000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1156252_e1156572_b00001_c20230208115718069000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:56:57.200000", "end_time": "2023-02-08T11:57:29.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1156572_e1157292_b00001_c20230208115738216000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1156572_e1157292_b00001_c20230208115738216000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:57:29.200000", "end_time": "2023-02-08T11:58:01.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1157292_e1158012_b00001_c20230208115818194000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1157292_e1158012_b00001_c20230208115818194000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:58:01.200000", "end_time": "2023-02-08T11:58:33.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1158012_e1158332_b00001_c20230208115837985000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1158012_e1158332_b00001_c20230208115837985000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:58:33.200000", "end_time": "2023-02-08T11:59:05.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1158332_e1159052_b00001_c20230208115918341000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1158332_e1159052_b00001_c20230208115918341000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:59:05.200000", "end_time": "2023-02-08T11:59:37.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1159052_e1159372_b00001_c20230208115957983000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1159052_e1159372_b00001_c20230208115957983000_drlu_ops.h5"}, {"start_time": "2023-02-08T11:59:37.200000", "end_time": "2023-02-08T12:00:09.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1159372_e1200092_b00001_c20230208120017590000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1159372_e1200092_b00001_c20230208120017590000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:00:09.200000", "end_time": "2023-02-08T12:00:41.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1200092_e1200412_b00001_c20230208120058642000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1200092_e1200412_b00001_c20230208120058642000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:00:41.200000", "end_time": "2023-02-08T12:01:13.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1200412_e1201132_b00001_c20230208120118131000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1200412_e1201132_b00001_c20230208120118131000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:01:13.200000", "end_time": "2023-02-08T12:01:45.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1201132_e1201452_b00001_c20230208120157708000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1201132_e1201452_b00001_c20230208120157708000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:01:45.200000", "end_time": "2023-02-08T12:02:17.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1201452_e1202172_b00001_c20230208120238505000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1201452_e1202172_b00001_c20230208120238505000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:02:17.200000", "end_time": "2023-02-08T12:02:49.200000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1202172_e1202492_b00001_c20230208120258137000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1202172_e1202492_b00001_c20230208120258137000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:02:49.200000", "end_time": "2023-02-08T12:03:21.100000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1202492_e1203211_b00001_c20230208120337761000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1202492_e1203211_b00001_c20230208120337761000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:03:21.100000", "end_time": "2023-02-08T12:03:53.100000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1203211_e1203531_b00001_c20230208120358165000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1203211_e1203531_b00001_c20230208120358165000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:03:53.100000", "end_time": "2023-02-08T12:04:25.100000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1203531_e1204251_b00001_c20230208120437760000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1203531_e1204251_b00001_c20230208120437760000_drlu_ops.h5"}, {"start_time": "2023-02-08T12:04:25.100000", "end_time": "2023-02-08T12:04:57.100000", "uri": "ssh://172.29.1.52/path/to/jpss/atms/rdr/RATMS-RNSCA_j01_d20230208_t1204251_e1204571_b00001_c20230208120518174000_drlu_ops.h5", "uid": "RATMS-RNSCA_j01_d20230208_t1204251_e1204571_b00001_c20230208120518174000_drlu_ops.h5"}]}"""  # noqa


TEST_ATMS_FILE_MESSAGE = """pytroll://atms/rdr/0/file file safusr.u@lxserv1043.smhi.se 2023-02-08T12:06:01.560943 v1.01 application/json {'start_time': datetime.datetime(2023, 2, 9, 9, 6, 10), 'end_time': datetime.datetime(2023, 2, 9, 9, 6, 42), 'orbit_number': 58481, 'platform_name': 'Suomi-NPP', 'sensor': 'atms', 'format': 'RDR', 'type': 'HDF5', 'data_processing_level': '0', 'uid': 'RATMS-RNSCA_npp_d20230209_t0906100_e0906420_b00001_c20230209090700529000_drlu_ops.h5', 'uri': 'ssh://172.29.1.52/san1/polar_in/direct_readout/npp/lvl0/RATMS-RNSCA_npp_d20230209_t0906100_e0906420_b00001_c20230209090700529000_drlu_ops.h5', 'variant': 'DR'}"""  # noqa


@pytest.fixture(scope="session")
def fake_cspp_workdir(tmp_path_factory):
    """Create a fake CSPP working dir."""
    return tmp_path_factory.mktemp("work")


@pytest.fixture
def fake_atms_posttroll_message():
    """Create and return a Posttroll message for ATMS."""
    yield Message.decode(rawstr=str(TEST_ATMS_COLLECTION_MESSAGE))


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_atms_dr_config_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)

    yield file_path


# Create the fake CSPP/ATMS bash script
FAKE_ATMS_BASH_SCRIPT = """#!/bin/bash

if [ -z "$CSPP_SDR_HOME" ]; then
  echo "CSPP_SDR_HOME must be set to the path where the CSPP software was installed."
  echo "export CSPP_SDR_HOME=/home/me/SDR_x_x"
  exit 1
fi

python $CSPP_SDR_HOME/atms/fake_adl_atms_script.py -vv "$@" || echo "ATMS SDR did not complete nominally"
"""

FAKE_ATMS_PYTHON_MAIN_SCRIPT = """
import argparse

if __name__ == "__main__":
    desc = "Dummy/fake program to mimic launching the CSPP/ATMS rdr-to-sdr script."
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-a', '--aggregate',
                        action="store_true", default=False, help="aggregate products with nagg")
    parser.add_argument('-d', '--debug', action="store_true", default=False,
                        help='enable debug mode on ADL and avoid cleaning workspace')
    parser.add_argument('-v', '--verbosity', action="count", default=0,
                        help='each occurrence increases verbosity 1 level through ERROR-WARNING-INFO-DEBUG')
    parser.add_argument('filenames', metavar='filename', type=str, nargs='+',
                        help='HDF5 ATMS RDR file/s to process')

    args = parser.parse_args()

    print(args.filenames)
"""


@pytest.fixture(scope="session")
def fake_adl_atms_scripts(tmp_path_factory):
    """Create a fake cspp/atms bash and python main script."""
    cspp_home_dir = tmp_path_factory.mktemp('CSPP')
    path = cspp_home_dir / 'atms'
    path.mkdir()
    filename = path / 'atms_sdr.sh'
    with open(filename, 'w') as fpt:
        fpt.write(FAKE_ATMS_BASH_SCRIPT)

    # Make executable:
    filename.chmod(stat.S_IRWXU)

    filename = cspp_home_dir / 'atms' / 'fake_adl_atms_script.py'
    with open(filename, 'w') as fpt:
        fpt.write(FAKE_ATMS_PYTHON_MAIN_SCRIPT)

    yield cspp_home_dir
