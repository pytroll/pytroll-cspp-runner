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
import datetime


def generate_cspp_sdr_granule_filename(prefix, satname, orbit_number, starttime):
    """Generate a filename that follows the NOAA CSPP SDR file name convention."""
    now = datetime.datetime.now()
    granule_length_sec = 84.2
    endtime = starttime + datetime.timedelta(seconds=granule_length_sec)
    msec_start = starttime.strftime('%f')[0:1]
    msec_end = endtime.strftime('%f')[0:1]

    return f'{prefix}_{satname}_d{starttime:%Y%m%d_t%H%M%S}{msec_start}_e{endtime:%H%M%S}{msec_end}_b{orbit_number}_c{now:%Y%m%d%H%M%S%f}_cspp_dev.h5'  # noqa


@pytest.fixture
def fake_empty_viirs_sdr_files(tmp_path):
    """Write fake empty viirs sdr files."""
    starttime = datetime.datetime(2023, 5, 10, 14, 30, 53, 800000)
    return create_full_sdr_list_from_time(tmp_path, starttime)


# @pytest.fixture
# def fake_empty_viirs_sdr_files_nine_seconds_deviation(tmp_path):
#     """Write fake empty viirs sdr files."""
#     starttime = datetime.datetime(2023, 5, 10, 14, 30, 53, 800000) + datetime.timedelta(seconds=9)
#     return create_full_sdr_list_from_time(tmp_path, starttime)


def create_full_sdr_list_from_time(sdr_dir, starttime):
    """Create a full list of SDR files."""
    satname = 'npp'
    orbit = '59761'
    filelist = []
    for prefix in ['GDNBO', 'GIMGO', 'GITCO', 'GMODO', 'GMTCO', 'IVCDB', 'SVDNB']:
        file_path = sdr_dir / generate_cspp_sdr_granule_filename(prefix, satname, orbit, starttime)
        file_path.touch()
        filelist.append(file_path)

    for mband in range(1, 17):
        prefix = f'SVM{mband:02d}'
        file_path = sdr_dir / generate_cspp_sdr_granule_filename(prefix, satname, orbit, starttime)
        file_path.touch()
        filelist.append(file_path)

    for iband in range(1, 6):
        prefix = f'SVI{iband:02d}'
        file_path = sdr_dir / generate_cspp_sdr_granule_filename(prefix, satname, orbit, starttime)
        file_path.touch()
        filelist.append(file_path)

    return filelist
