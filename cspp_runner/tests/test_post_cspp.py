# Copyright (c) 2021, 2023 pytroll-cspp-runner developers

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

"""Tests for post-cspp module."""

import logging
import os
import pytest
from datetime import datetime, timedelta

from cspp_runner.post_cspp import pack_sdr_files
from cspp_runner.post_cspp import get_sdr_files

# Files from one granule in directory npp_20230510_1420_59761:

# GDNBO_npp_d20230510_t1430538_e1432180_b59761_c20230511111454905910_cspp_dev.h5

# noaa21_20230510_1027_02570
# GDNBO_j02_d20230510_t1040279_e1041526_b02570_c20230510105028602976_cspp_dev.h5


def test_pack_sdr_files(tmp_path, caplog):
    """Test packing/placing the SDR files in a separate directory."""
    # create dummy source file
    p = tmp_path / "source" / "sdr.h5"
    p.parent.mkdir(exist_ok=True, parents=True)
    p.touch()
    dest = tmp_path / "path" / "to" / "sdr_dir"

    with caplog.at_level(logging.DEBUG):
        newnames = pack_sdr_files(
            [p],
            os.fspath(dest),
            "subdir")
    assert "Number of SDR files: 1" in caplog.text
    assert (dest / "subdir" / "sdr.h5").exists()
    assert len(newnames) == 1
    assert isinstance(newnames[0], str)


def test_get_sdr_files_no_kwargs(fake_empty_viirs_sdr_files):
    """Test get the list of SDR filenames from filenames on disk."""
    basedir = fake_empty_viirs_sdr_files[0].parent
    result = get_sdr_files(basedir)

    assert len(result) == len(fake_empty_viirs_sdr_files)
    for i in range(len(fake_empty_viirs_sdr_files)):
        assert result[i] in fake_empty_viirs_sdr_files


def test_get_sdr_files_kwargs_platform_name_and_starttime(fake_empty_viirs_sdr_files):
    """Test get the list of SDR filenames from filenames on disk."""
    basedir = fake_empty_viirs_sdr_files[0].parent
    start_time = datetime(2023, 5, 10, 14, 30, 53, 800000)

    result = get_sdr_files(basedir, platform_name='Suomi-NPP', start_time=start_time)

    assert len(result) == len(fake_empty_viirs_sdr_files)
    for i in range(len(fake_empty_viirs_sdr_files)):
        assert result[i] in fake_empty_viirs_sdr_files


def test_get_sdr_files_kwargs_starttime_with_timedeviation_fails(fake_empty_viirs_sdr_files, caplog):
    """Test get the list of SDR filenames from filenames on disk."""
    basedir = fake_empty_viirs_sdr_files[0].parent
    start_time = datetime(2023, 5, 10, 14, 30, 53, 800000) + timedelta(seconds=10)

    with caplog.at_level(logging.ERROR):
        result = get_sdr_files(basedir, platform_name='Suomi-NPP', start_time=start_time)

    assert "No or not enough SDR files found matching the RDR granule" in caplog.text
    assert len(result) == 0


@pytest.mark.parametrize('seconds', (1, 3, 6, 9, pytest.param(11, marks=pytest.mark.xfail)))
def test_get_sdr_files_kwargs_starttime_with_timedeviation_okay(fake_empty_viirs_sdr_files, seconds, caplog):
    """Test get the list of SDR filenames from filenames on disk."""
    basedir = fake_empty_viirs_sdr_files[0].parent
    start_time = datetime(2023, 5, 10, 14, 30, 53, 800000) + timedelta(seconds=seconds)

    result = get_sdr_files(basedir, platform_name='Suomi-NPP', start_time=start_time,
                           time_tolerance=timedelta(seconds=10))

    assert len(result) == len(fake_empty_viirs_sdr_files)
    for i in range(len(fake_empty_viirs_sdr_files)):
        assert result[i] in fake_empty_viirs_sdr_files
