#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2020 - 2023 Pytroll

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

"""Unittests of the helper functions and utilities for the real-time CSPP SDR processing."""

from cspp_runner import get_sdr_times, get_datetime_from_filename
from cspp_runner.cspp_utils import CSPPAncillaryDataUpdater

from datetime import datetime, timedelta
from unittest import TestCase
import logging
import os
from freezegun import freeze_time
import pytest


class TestTimeExtraction(TestCase):
    """Class for testing extraction of observation times from the file names."""

    def test_get_sdr_times(self):
        """Test get observation times from the SDR files."""
        filename = 'GMODO_npp_d20120405_t0037099_e0038341_b00001_c20120405124731856767_cspp_dev.h5'
        start_time, end_time = get_sdr_times(filename)
        assert start_time == datetime(2012, 4, 5, 0, 37, 9, 900000)
        assert end_time == datetime(2012, 4, 5, 0, 38, 34, 100000)

    def test_get_sdr_times_over_midnight(self):
        """Test get observation times from the SDR files, when pass crosses midnight."""
        filename = 'GMODO_npp_d20120405_t2359099_e0000341_b00001_c20120405124731856767_cspp_dev.h5'
        start_time, end_time = get_sdr_times(filename)
        assert start_time == datetime(2012, 4, 5, 23, 59, 9, 900000)
        assert end_time == datetime(2012, 4, 6, 0, 0, 34, 100000)
        assert get_datetime_from_filename(filename) == start_time


def test_update_lut_files4cspp_no_script_provided(monkeypatch, tmp_path, caplog):
    """Test the update of LUT files over internet for CSPP processing."""
    lutdir = tmp_path / 'my_lut_dir'
    lut_script = None
    kwargs = {'url_download_trial_frequency_hours': 2,
              'thr_lut_files_age_days': 1,
              'lut_dir': lutdir,
              'mirror_jpss_luts': lut_script,
              'url_jpss_remote_lut_dir': 'http://jpssdb.ssec.wisc.edu/cspp_luts_v_6_0/'
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    with caplog.at_level(logging.DEBUG):
        updater.update_luts()

    log_output = "No LUT update script provided. No LUT updating will be attempted."
    assert log_output in caplog.text


# def test_update_lut_files4cspp_no_script_provided(tmp_path, caplog):
#     """Test the update of LUT files over internet for CSPP processing."""
#     lutdir = tmp_path / 'my_lut_dir'
#     lut_script = 'ls'
#     kwargs = {'url_download_trial_frequency_hours': 2,
#               'thr_lut_files_age_days': 1,
#               'lut_update_stampfile_prefix': tmp_path / 'lut_update',
#               'lut_dir': lutdir,
#               'mirror_jpss_luts': lut_script,
#               'url_jpss_remote_lut_dir': 'http://jpssdb.ssec.wisc.edu/cspp_luts_v_6_0/'
#               }
#     updater = CSPPAncillaryDataUpdater(**kwargs)

#     with caplog.at_level(logging.DEBUG):
#         updater.update_luts()

#     log_output = "No LUT update script provided. No LUT updating will be attempted."
#     assert log_output in caplog.text


def test_initialize_environment_variable_is_none(monkeypatch, tmp_path, caplog):
    """Test update luts/anc data fails when some env variable is set to None."""
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))
    lutdir = tmp_path / 'my_lut_dir'
    lutdir.mkdir()
    kwargs = {'lut_update_stampfile_prefix': os.fspath(lutdir / "stampfile"),
              'lut_dir': lutdir,
              'mirror_jpss_luts': "true",
              'url_jpss_remote_lut_dir': None,
              }

    with caplog.at_level(logging.DEBUG), pytest.raises(EnvironmentError):
        _ = CSPPAncillaryDataUpdater(**kwargs)

    assert "Environments set:" in caplog.text


def test_initialize_missing_env(monkeypatch, tmp_path):
    """Test initialize object fails when env missing."""
    monkeypatch.delenv("CSPP_WORKDIR", raising=False)
    lutdir = tmp_path
    kwargs = {'lut_update_stampfile_prefix': os.fspath(lutdir / "stampfile"),
              'mirror_jpss_luts': "true",
              'url_jpss_remote_lut_dir': "gopher://dummy/location",
              }

    # should raise exception when no workdir set
    with pytest.raises(EnvironmentError):
        _ = CSPPAncillaryDataUpdater(**kwargs)


@freeze_time('2023-05-19 12:00:00')
def test_update_luts_nominal(monkeypatch, tmp_path, caplog):
    """Test update LUTs - nominal case."""
    label = 'LUT'
    lutdir = tmp_path / 'my_lut_dir'
    lutdir.mkdir()
    lut_script = "echo"
    kwargs = {'url_download_trial_frequency_hours': 2,
              'thr_lut_files_age_days': 1,
              'lut_update_stampfile_prefix': os.fspath(lutdir / "stampfile"),
              'lut_dir': lutdir,
              'mirror_jpss_luts': lut_script,
              'url_jpss_remote_lut_dir': "gopher://dummy/location",
              'url_jpss_remote_anc_dir': "gopher://dummy/other_location",
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    with caplog.at_level(logging.INFO):
        updater.update_luts()

    assert f"Download command for {label:s}" in caplog.text
    assert f"Directory {lutdir}" in caplog.text
    filepath_prefix = str(lutdir / "stampfile")
    assert f"LUT downloaded. LUT-update timestamp file = {filepath_prefix}" in caplog.text

    fake_now = datetime(2023, 5, 19, 12, 0, 0)
    expected = lutdir / f"stampfile.{fake_now:%Y%m%d%H%M}"
    assert expected.exists()


@freeze_time('2023-05-19 12:00:00')
def test_update_luts_error(monkeypatch, tmp_path, caplog):
    """Check that a failed LUT update is logged to stderr.

    And that the stampfile is NOT updated in this case.
    """
    lutdir = tmp_path / 'empty'
    lutdir.mkdir()
    lut_script = "false"
    kwargs = {'url_download_trial_frequency_hours': 2,
              'thr_lut_files_age_days': 1,
              'lut_update_stampfile_prefix': os.fspath(lutdir / "stampfile"),
              'lut_dir': lutdir,
              'mirror_jpss_luts': lut_script,
              'url_jpss_remote_lut_dir': "gopher://dummy/location",
              'url_jpss_remote_anc_dir': "gopher://dummy/other_location",
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    with caplog.at_level(logging.ERROR):
        updater.update_luts()

    assert "exit code 1" in caplog.text

    fake_now = datetime(2023, 5, 19, 12, 0, 0)
    expected = tmp_path / f"stampfile.{fake_now:%Y%m%d%H%M}"
    assert not expected.exists()


@freeze_time('2023-05-19 12:00:00')
def test_update_ancillary_data_nominal(monkeypatch, tmp_path, caplog):
    """Test update Ancillary data - nominal case."""
    label = 'ANC'
    ancdir = tmp_path / 'empty'
    ancdir.mkdir()
    anc_script = "echo"
    kwargs = {'anc_update_stampfile_prefix': os.fspath(ancdir / "stampfile"),
              'anc_dir': ancdir,
              'mirror_jpss_ancillary': anc_script,
              'url_jpss_remote_anc_dir': "gopher://dummy/location",
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    with caplog.at_level(logging.INFO):
        updater.update_ancillary_data()

    assert f"Download command for {label:s}" in caplog.text
    filepath_prefix = str(ancdir / "stampfile")
    assert f"ANC downloaded. ANC-update timestamp file = {filepath_prefix}" in caplog.text

    fake_now = datetime(2023, 5, 19, 12, 0, 0)
    expected = ancdir / f"stampfile.{fake_now:%Y%m%d%H%M}"
    assert expected.exists()


@freeze_time('2023-05-19 12:00:00')
def test_update_ancillary_data_error(monkeypatch, tmp_path, caplog):
    """Check that a failed Ancillary data update is logged to stderr.

    And that the stampfile is NOT updated in this case.
    """
    ancdir = tmp_path / 'empty'
    ancdir.mkdir()
    anc_script = "false"
    kwargs = {'anc_update_stampfile_prefix': os.fspath(ancdir / "stampfile"),
              'anc_dir': ancdir,
              'mirror_jpss_ancillary': anc_script,
              'url_jpss_remote_anc_dir': "gopher://dummy/location",
              'url_jpss_remote_lut_dir': "gopher://dummy/other_location",
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    with caplog.at_level(logging.ERROR):
        updater.update_ancillary_data()

    assert "exit code 1" in caplog.text

    fake_now = datetime(2023, 5, 19, 12, 0, 0)
    expected = ancdir / f"stampfile.{fake_now:%Y%m%d%H%M}"
    assert not expected.exists()


def test_check_lut_files_virgin(monkeypatch, tmp_path):
    """Test check LUT files, virgin case."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    kwargs = {'url_download_trial_frequency_hours': 1,
              'thr_lut_files_age_days': 5,
              'lut_update_stampfile_prefix': "prefix",
              'lut_dir': os.fspath(empty_dir),
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    res = updater.check_lut_files()
    assert not res


def test_check_lut_files_uptodate(monkeypatch, tmp_path):
    """Test check LUT files, everything up to date."""
    # create fake stamp files
    now = datetime.now()
    yesterday = datetime.now() - timedelta(days=1)
    yesteryear = datetime.now() - timedelta(days=400)
    stamp = tmp_path / "stamp"
    for dt in (yesteryear, yesterday, now):
        fn = stamp.with_suffix(f".{dt:%Y%m%d%H%M}")
        fn.touch()

    kwargs = {'url_download_trial_frequency_hours': 1,
              'thr_lut_files_age_days': 5,
              'lut_update_stampfile_prefix': os.fspath(stamp),
              'lut_dir': "irrelevant"
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    res = updater.check_lut_files()
    assert res


def test_check_lut_files_outofdate(monkeypatch, tmp_path, caplog):
    """Test check LUT files, out of date case."""
    # create fake stamp file
    yesteryear = datetime.now() - timedelta(days=400)
    stamp = tmp_path / "stamp"
    fn = stamp.with_suffix(f".{yesteryear:%Y%m%d%H%M}")
    fn.touch()
    # create fake LUT from yesteryear
    lut_dir = tmp_path / "lut"
    lut_dir.mkdir()
    lutfile = lut_dir / "dummy"
    lutfile.touch()
    os.utime(lutfile, (yesteryear.timestamp(),)*2)

    kwargs = {'url_download_trial_frequency_hours': 1,
              'thr_lut_files_age_days': 5,
              'lut_update_stampfile_prefix': os.fspath(stamp),
              'lut_dir': os.fspath(lut_dir)
              }
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    updater = CSPPAncillaryDataUpdater(**kwargs)
    res = updater.check_lut_files()
    assert not res
