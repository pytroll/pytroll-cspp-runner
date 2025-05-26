#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Pytroll developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

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

"""Testing the ATMS processing."""

import os
import logging
from glob import glob
from datetime import datetime

from trollsift import Parser

from cspp_runner.config import read_config

from cspp_runner.atms_rdr2sdr_runner import get_filepaths
from cspp_runner.atms_rdr2sdr_runner import run_atms_from_message
from cspp_runner.atms_rdr2sdr_runner import get_filelist_from_collection
from cspp_runner.atms_rdr2sdr_runner import move_files_to_destination
from cspp_runner.atms_rdr2sdr_runner import _fix_orbit_number


ATMS_FILENAMES = ['RATMS-RNSCA_j01_d20230208_t1154172_e1154492_b00001_c20230208115457829000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1154492_e1155212_b00001_c20230208115538023000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1155212_e1155532_b00001_c20230208115558220000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1155532_e1156252_b00001_c20230208115638170000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1156252_e1156572_b00001_c20230208115718069000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1156572_e1157292_b00001_c20230208115738216000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1157292_e1158012_b00001_c20230208115818194000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1158012_e1158332_b00001_c20230208115837985000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1158332_e1159052_b00001_c20230208115918341000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1159052_e1159372_b00001_c20230208115957983000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1159372_e1200092_b00001_c20230208120017590000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1200092_e1200412_b00001_c20230208120058642000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1200412_e1201132_b00001_c20230208120118131000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1201132_e1201452_b00001_c20230208120157708000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1201452_e1202172_b00001_c20230208120238505000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1202172_e1202492_b00001_c20230208120258137000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1202492_e1203211_b00001_c20230208120337761000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1203211_e1203531_b00001_c20230208120358165000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1203531_e1204251_b00001_c20230208120437760000_drlu_ops.h5',
                  'RATMS-RNSCA_j01_d20230208_t1204251_e1204571_b00001_c20230208120518174000_drlu_ops.h5']


def test_run_atms_from_message(caplog, monkeypatch, fake_cspp_workdir,
                               fake_atms_posttroll_message, fake_adl_atms_scripts):
    """Test launch and run the ATMS processing from a Posttroll message."""
    monkeypatch.setenv("CSPP_WORKDIR", str(fake_cspp_workdir))

    cspp_homedir = fake_adl_atms_scripts
    monkeypatch.setenv("CSPP_SDR_HOME", str(cspp_homedir))

    mypath = cspp_homedir / 'atms'
    path_env = os.environ.get('PATH')
    monkeypatch.setenv("PATH", path_env + ":" + str(mypath))

    sdr_call = 'atms_sdr.sh'
    sdr_options = ['-d', '-a']
    with caplog.at_level(logging.DEBUG):
        dirpath = run_atms_from_message(fake_atms_posttroll_message, sdr_call, sdr_options)

    assert os.path.dirname(dirpath) == str(fake_cspp_workdir)
    res = caplog.text.strip().split('\n')
    assert len(res) == 4

    for atmsfile in ATMS_FILENAMES:
        assert atmsfile in res[0]
        assert atmsfile in res[1]

    assert "Seconds process time:" in res[2]
    assert "Seconds wall clock time:" in res[3]


def test_get_filelist_from_collection(fake_atms_posttroll_message):
    """Test launch and run the ATMS processing from a Posttroll message."""
    collection = fake_atms_posttroll_message.data.get('collection')

    files = get_filelist_from_collection(collection)

    assert len(files) == 20
    bnames = [os.path.basename(item) for item in files]
    assert bnames == ATMS_FILENAMES


def test_get_filepaths(fake_yamlconfig_file, fake_atms_sdr_files_several_passes):
    """Test get the filepaths of the ATMS SDR files produced from CSPP."""
    config = read_config(fake_yamlconfig_file)

    patterns = config['sdr_file_patterns']
    fake_message_data = {}
    fake_message_data['orbit_number'] = 27088
    fake_message_data['platform_name'] = 'NOAA-20'
    fake_message_data['start_time'] = datetime.strptime("2023-02-09T13:17:20.600000", "%Y-%m-%dT%H:%M:%S.%f")
    fake_message_data['end_time'] = datetime.strptime("2023-02-09T13:25:52.600000", "%Y-%m-%dT%H:%M:%S.%f")
    files = get_filepaths(str(fake_atms_sdr_files_several_passes), fake_message_data, patterns)

    assert len(files) == 3
    p__ = Parser(patterns[0])
    result = p__.parse(os.path.basename(files[0]))
    assert result['platform_shortname'] == 'j01'
    assert result['orbit'] == 27088
    assert result['start_time'] == datetime(2023, 2, 9, 13, 17, 54, 600000)


def test_move_files_to_destination_dir_is_str(fake_yamlconfig_file, fake_sdr_homedir, fake_atms_sdr_files_one_pass):
    """Test move the ATMS SDR files to a destination dir."""
    config = read_config(fake_yamlconfig_file)
    patterns = config['sdr_file_patterns']

    sdr_file_paths = glob(str(fake_atms_sdr_files_one_pass / '*h5'))
    expected = [os.path.basename(f) for f in sdr_file_paths]
    expected.sort()

    filelist = move_files_to_destination(sdr_file_paths, patterns, str(fake_sdr_homedir))

    assert len(filelist) == 3
    assert os.path.basename(os.path.normpath(os.path.dirname(filelist[0]))) == "noaa20_20230209_1317_27088"
    bnames = [os.path.basename(f) for f in filelist]
    bnames.sort()

    assert bnames == expected


def test_move_files_to_destination_pathlib(fake_yamlconfig_file, fake_sdr_homedir, fake_atms_sdr_files_one_pass):
    """Test move the ATMS SDR files to a destination dir."""
    config = read_config(fake_yamlconfig_file)
    patterns = config['sdr_file_patterns']

    sdr_file_paths = glob(str(fake_atms_sdr_files_one_pass / '*h5'))
    expected = [os.path.basename(f) for f in sdr_file_paths]
    expected.sort()

    filelist = move_files_to_destination(sdr_file_paths, patterns, fake_sdr_homedir)

    assert len(filelist) == 3
    assert os.path.basename(os.path.normpath(os.path.dirname(filelist[0]))) == "noaa20_20230209_1317_27088"
    bnames = [os.path.basename(f) for f in filelist]
    bnames.sort()

    assert bnames == expected


def test_fix_orbit_number(fake_yamlconfig_file, fake_atms_sdr_files_one_pass):
    """Test fixing the orbit number from the SDR filenames."""
    config = read_config(fake_yamlconfig_file)
    patterns = config['sdr_file_patterns']

    sdr_files = glob(str(fake_atms_sdr_files_one_pass / '*h5'))

    result = _fix_orbit_number(sdr_files, patterns)
    assert result == 27088
