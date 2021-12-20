# Copyright (c) 2021 pytroll-cspp-runner developers

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

"""Tests for runner module."""

import logging
import os
import unittest.mock

import posttroll.message
import pytest


@pytest.fixture
def fakefile(tmp_path):
    """Create a fake empty viirs granule and return its path."""
    p = (tmp_path /
         "RNSCA-RVIRS_npp_d20211217_t0959003_"
         "e1011484_b00001_c20211217101206466000_all-_dev.h5")
    p.touch()
    return p


def test_run_fullswath(tmp_path, fakefile, caplog):
    """Test the runner with a single fullswath file."""
    from cspp_runner.runner import ViirsSdrProcessor
    msg = posttroll.message.Message(
            rawstr="pytroll://file/snpp/viirs/direktempfang file "
            "pytroll@oflks333.dwd.de 2021-12-20T15:01:02.780614 v1.01 "
            'application/json {"path": "", "start_time": '
            '"2021-12-17T09:59:00.300000", "end_time": '
            '"2021-12-17T10:11:48.400000", "orbit": 1, "processing_time": '
            '"2021-12-17T10:12:06.466000", "uri": '
            f'"{fakefile!s}", "uid": '
            '"RNSCA-RVIRS_npp_d20211217_t0959003_e1011484_b00001_'
            'c20211217101206466000_all-_dev.h5", "sensor": ["viirs"], '
            '"platform_name": "Suomi-NPP"}')

    with unittest.mock.patch("cspp_runner.runner.ThreadPool") as crT, \
         unittest.mock.patch("cspp_runner.runner.fix_rdrfile") as csr:
        csr.return_value = (os.fspath(fakefile), 42)
        vsp = ViirsSdrProcessor(1, tmp_path / "outdir")
        with caplog.at_level(logging.ERROR):
            vsp.run(msg)
        assert crT().apply_async.call_count == 1
        assert caplog.text == ""


def test_publish():
    """Test publishing SDR."""
    from cspp_runner.runner import publish_sdr
    from posttroll.publisher import Publish
    with Publish("bolungarvík", 0) as publisher:
        publish_sdr(
                publisher,
                ["/foo/bar"],
                {},
                "wonderland",
                "treasure/collected/complete",
                orbit=42)
