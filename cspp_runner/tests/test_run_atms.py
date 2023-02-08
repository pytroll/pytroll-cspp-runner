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


import pytest
import os
import logging

from cspp_runner.atms_rdr2sdr_runner import run_atms_from_message
from cspp_runner.atms_rdr2sdr_runner import get_filelist_from_collection

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
    with caplog.at_level(logging.INFO):
        run_atms_from_message(fake_atms_posttroll_message, sdr_call, sdr_options)

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
