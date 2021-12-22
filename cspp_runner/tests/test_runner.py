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

import datetime
import logging
import os
import signal
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


@pytest.fixture
def fakemessage(fakefile):
    return posttroll.message.Message(
            rawstr="pytroll://file/snpp/viirs/direktempfang file "
            "pytroll@oflks333.dwd.de 2021-12-20T15:01:02.780614 v1.01 "
            'application/json {"path": "", "start_time": '
            '"2021-12-17T09:59:00.300000", "end_time": '
            '"2021-12-17T10:11:48.400000", "orbit_number": 1, "processing_time": '
            '"2021-12-17T10:12:06.466000", "uri": '
            f'"{fakefile!s}", "uid": '
            '"RNSCA-RVIRS_npp_d20211217_t0959003_e1011484_b00001_'
            'c20211217101206466000_all-_dev.h5", "sensor": ["viirs"], '
            '"platform_name": "Suomi-NPP"}')


@pytest.fixture
def fake_result_names(tmp_path):
    p = tmp_path / "results"
    all = []
    for f in [
        "GMTCO_j01_d20211221_t1436067_e1437312_b21200_c20211221144949626416_cspp_dev.h5",
        "SVM12_j01_d20211221_t1436067_e1437312_b21200_c20211221145100921548_cspp_dev.h5",
        "SVM10_j01_d20211221_t1436067_e1437312_b21200_c20211221145100419360_cspp_dev.h5",
        "SVM02_j01_d20211221_t1436067_e1437312_b21200_c20211221145058798219_cspp_dev.h5",
        "SVM09_j01_d20211221_t1436067_e1437312_b21200_c20211221145100251266_cspp_dev.h5"]:
        new = p / f
        all.append(new)
    return all


@pytest.fixture
def fake_results(tmp_path, fake_result_names):
    p = tmp_path / "results"
    p.mkdir(parents=True, exist_ok=True)
    created = []
    for f in fake_result_names:
        new = p / f
        new.touch()
        created.append(os.fspath(new))
    return created


def test_run_fullswath(tmp_path, fakefile, fakemessage, caplog):
    """Test the runner with a single fullswath file."""
    from cspp_runner.runner import ViirsSdrProcessor

    with unittest.mock.patch("cspp_runner.runner.ThreadPool") as crT, \
         unittest.mock.patch("cspp_runner.runner.fix_rdrfile") as csr:
        csr.return_value = (os.fspath(fakefile), 42)
        vsp = ViirsSdrProcessor(1, tmp_path / "outdir")
        with caplog.at_level(logging.ERROR):
            vsp.run(fakemessage, "true", [])
        assert crT().apply_async.call_count == 1
        assert caplog.text == ""


def test_publish(fake_results):
    """Test publishing SDR."""
    from cspp_runner.runner import publish_sdr
    from posttroll.publisher import Publish

    class FakePublisher:
        def __init__(self):
            self.messages = []

        def send(self, msg):
            self.messages.append(msg)

    fake_publisher = FakePublisher()

    publish_sdr(
            fake_publisher,
            fake_results,
            {"orbit_number": 21200},
            "wonderland",
            "test",
            "treasure/collected/complete",
            orbit=42)
    assert len(fake_publisher.messages) == 1
    msg = fake_publisher.messages[0]
    assert msg.startswith(
            "pytroll://treasure/collected/complete/SDR/1B/wonderland"
            "/test/polar/direct_readout dataset")


@pytest.mark.parametrize(
        "funcname", ["update_lut_files", "update_ancillary_files"])
def test_update_missing_env(monkeypatch, tmp_path, funcname):
    """Test updating fails when env missing."""
    monkeypatch.delenv("CSPP_WORKDIR", raising=False)
    import cspp_runner.runner
    updater = getattr(cspp_runner.runner, funcname)
    # should raise exception when no workdir set
    with pytest.raises(EnvironmentError):
        updater(
                "gopher://dummy/location",
                os.fspath(tmp_path / "stampfile"),
                "true")


@pytest.mark.parametrize(
        "funcname,label", [("update_lut_files", "LUT"),
                           ("update_ancillary_files", "ANC")])
def test_update_nominal(monkeypatch, tmp_path, caplog, funcname, label):
    """Test update nominal case."""
    import cspp_runner.runner
    updater = getattr(cspp_runner.runner, funcname)
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))
    with caplog.at_level(logging.INFO):
        updater("gopher://dummy/location",
                os.fspath(tmp_path / "stampfile"),
                "echo")
    assert f"Download command for {label:s}" in caplog.text
    assert caplog.text.split("\n")[2].endswith(
            f"-W {tmp_path / 'env'!s}")
    assert "downloaded" in caplog.text
    # I tried to use the technique at
    # https://stackoverflow.com/a/20503374/974555 to patch datetime.now, but
    # importing pandas fails if I do so, as pandas apparently also does some
    # trickery.  Therefore instead hope that `now` was recent enough to fit in the
    # previous minute at worst.
    now = datetime.datetime.utcnow()
    justnow = now - datetime.timedelta(seconds=5)
    exp1 = tmp_path / f"stampfile.{now:%Y%m%d%H%M}"
    exp2 = tmp_path / f"stampfile.{justnow:%Y%m%d%H%M}"
    assert exp1.exists() or exp2.exists()


@pytest.mark.parametrize(
        "funcname", ["update_lut_files", "update_ancillary_files"])
def test_update_error(monkeypatch, tmp_path, caplog, funcname):
    """Check that a failed LUT update is logged to stderr.

    And that the stampfile is NOT updated in this case."""
    import cspp_runner.runner
    updater = getattr(cspp_runner.runner, funcname)
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))
    with caplog.at_level(logging.ERROR):
        updater(
                "gother://dummy/location",
                os.fspath(tmp_path / "stampfile"),
                "false")
    assert "exit code 1" in caplog.text
    now = datetime.datetime.utcnow()
    justnow = now - datetime.timedelta(seconds=5)
    exp1 = tmp_path / f"stampfile.{now:%Y%m%d%H%M}"
    exp2 = tmp_path / f"stampfile.{justnow:%Y%m%d%H%M}"
    assert not exp1.exists()
    assert not exp2.exists()


def test_check_lut_files_virgin(tmp_path):
    """Test check LUT files, virgin case."""
    import cspp_runner.runner
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    res = cspp_runner.runner.check_lut_files(
            5, 1, "prefix", os.fspath(empty_dir))
    assert not res


def test_check_lut_files_uptodate(tmp_path):
    """Test check LUT files, everything up to date."""
    import cspp_runner.runner
    # create fake stamp files
    now = datetime.datetime.now()
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesteryear = datetime.datetime.now() - datetime.timedelta(days=400)
    stamp = tmp_path / "stamp"
    for dt in (yesteryear, yesterday, now):
        fn = stamp.with_suffix(f".{dt:%Y%m%d%H%M}")
        fn.touch()
    res = cspp_runner.runner.check_lut_files(
            5, 1, os.fspath(stamp), "irrelevant")
    assert res


def test_check_lut_files_outofdate(tmp_path, caplog):
    """Test check LUT files, out of date case."""
    import cspp_runner.runner
    # create fake stamp file
    yesteryear = datetime.datetime.now() - datetime.timedelta(days=400)
    stamp = tmp_path / "stamp"
    fn = stamp.with_suffix(f".{yesteryear:%Y%m%d%H%M}")
    fn.touch()
    # create fake LUT from yesteryear
    lut_dir = tmp_path / "lut"
    lut_dir.mkdir()
    lutfile = lut_dir / "dummy"
    lutfile.touch()
    os.utime(lutfile, (yesteryear.timestamp(),)*2)
    res = cspp_runner.runner.check_lut_files(
            5, 1,
            os.fspath(stamp),
            os.fspath(lut_dir))
    assert not res


def test_run_cspp():
    """Test running CSPP."""
    import cspp_runner.runner
    cspp_runner.runner.run_cspp("true", [])


def test_spawn_cspp_nominal(tmp_path, caplog, fake_result_names):
    """Test spawning CSPP successfully."""
    import cspp_runner.runner
    def fake_run_cspp(call, args, *rdrs):
        p = tmp_path / "working_dir"
        p.mkdir()
        for f in fake_result_names:
            (p / f.name).touch()
        return os.fspath(p)
    with unittest.mock.patch("cspp_runner.runner.run_cspp") as crr:
        crr.side_effect = fake_run_cspp
        with caplog.at_level(logging.DEBUG):
            (wd, rf) = cspp_runner.runner.spawn_cspp(
                os.fspath(tmp_path /
                    "RNSCA-RVIRS_j01_d20211221_t1436057_e1444378_b21199_c20211221144433345000_all-_dev.h5"),
                viirs_sdr_call="touch",
                viirs_sdr_options=[],
                granule_time_tolerance=10)
    assert "Start CSPP" in caplog.text
    assert "CSPP probably failed" not in caplog.text
    assert "Number of results files" in caplog.text
    assert len(rf) == 5


def test_spawn_cspp_failure(tmp_path, caplog):
    """Test spawning CSPP unsuccessfully."""
    import cspp_runner.runner
    with caplog.at_level(logging.WARNING):
        (wd, rf) = cspp_runner.runner.spawn_cspp(
            *[os.fspath(tmp_path / f"file{i:d})")
                for i in range(4)],
            viirs_sdr_call="false",
            viirs_sdr_options=[])
    assert len(rf) == 0
    assert "CSPP probably failed!" in caplog.text


def test_rolling_runner(tmp_path, caplog, monkeypatch, fakemessage,
                        fake_results):
    """Test NPP rolling runner."""
    from cspp_runner.runner import npp_rolling_runner

    class TimeOut(Exception):
        pass

    def handler(signum, frame):
        raise TimeOut()

    fake_workdir = tmp_path / "workdir"
    def fake_spawn_cspp(current_granule, *glist, viirs_sdr_call,
                        viirs_sdr_options):
        fake_workdir.mkdir(exist_ok=True, parents=True)
        return (os.fspath(fake_workdir), fake_results)

    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(fake_workdir))
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(2)
    with unittest.mock.patch("posttroll.subscriber.Subscribe") as psS, \
         unittest.mock.patch("cspp_runner.runner.Publish") as crP, \
         unittest.mock.patch("cspp_runner.runner.spawn_cspp", new=fake_spawn_cspp) as crs, \
         caplog.at_level(logging.DEBUG):
        psS.return_value.__enter__.return_value.recv.return_value = [fakemessage]
        crs.return_value = (os.fspath(fake_workdir), fake_results)
        try:
            npp_rolling_runner(7, 24,
                               os.fspath(tmp_path / "stamp_lut"),
                               os.fspath(tmp_path / "lut"),
                               "gopher://example.org/luts", "true",
                               "gopher://example.org/ancs",
                               os.fspath(tmp_path / "stamp_anc"),
                               "true", "/file/available/rdr", "earth",
                               "test",
                               "/product/available/sdr", tmp_path / "sdr/results",
                               "true", [], 2)
        except TimeOut:
            pass # probably all is fine
        else:
            assert False  # should never get here
    assert "Dynamic ancillary data will be updated" in caplog.text
