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

"""Tests for runner module."""

import logging
import os
import signal
from unittest.mock import patch
import pathlib

import posttroll.message
import pytest


class FakePublisher:
    """Fake Publisher class."""

    def __init__(self):
        """Initialize the Fake Publisher class."""
        self.messages = []

    def send(self, msg):
        """Fake sending a message."""
        self.messages.append(msg)


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
    """Fake an incoming message with an RDR file for CSPP."""
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
    """Make a full list of CSPP SDR filenames."""
    p = tmp_path / "results"
    all = []
    for lbl in ["GMTCO", "SVM02", "SVM09", "SVM10", "SVM12"]:
        for f in [
                f"{lbl:s}_j01_d20211229_t1342527_e1344173_b21313_c20211229140342734674_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1344185_e1345430_b21313_c20211229140341407055_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1345442_e1347070_b21313_c20211229140319026930_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1347082_e1348327_b21313_c20211229140346771475_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1348340_e1349585_b21313_c20211229140336541537_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1349597_e1351242_b21313_c20211229140303590485_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1351255_e1352482_b21313_c20211229140800466176_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1352495_e1354140_b21313_c20211229142321678073_cspp_dev.h5",
                f"{lbl:s}_j01_d20211229_t1354152_e1355397_b21313_c20211229142320304291_cspp_dev.h5"]:
            new = p / f
            all.append(new)
    return all


@pytest.fixture
def fake_results(tmp_path, fake_result_names):
    """Fake a CSPP result of SDR files."""
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

    fake_publisher = FakePublisher()

    with patch("cspp_runner.runner.ThreadPool") as crT, \
            patch("cspp_runner.runner.fix_rdrfile") as csr:
        csr.return_value = (os.fspath(fakefile), 42)
        vsp = ViirsSdrProcessor(1, tmp_path / "outdir", 'fake_topic')

        with caplog.at_level(logging.ERROR):
            vsp.run(fakemessage, fake_publisher, "true", [])
        assert crT().apply_async.call_count == 1
        assert caplog.text == ""


def test_publish(tmp_path, fake_results, fakemessage, fake_empty_viirs_sdr_files):
    """Test publishing SDR."""
    from cspp_runner.runner import ViirsSdrProcessor

    fake_publisher = FakePublisher()

    with patch("cspp_runner.runner.ThreadPool"), \
            patch("cspp_runner.post_cspp.get_sdr_files") as get_sdr_files:
        vsp = ViirsSdrProcessor(1, tmp_path / "outdir", 'fake_topic')
        get_sdr_files.return_value = fake_empty_viirs_sdr_files
        vsp.orbit_number = 42
        vsp.message_data = fakemessage.data

        vsp.publish_sdr(fake_publisher, fake_results)

    assert len(fake_publisher.messages) == 1
    msg = fake_publisher.messages[0]

    assert msg.startswith("pytroll://fake_topic/SDR/1B/polar/direct_readout")
    assert '"end_time": "2021-12-29T13:55:39.700000"' in msg
    assert '"start_time": "2021-12-29T13:42:52.700000"' in msg


def test_run_cspp(monkeypatch, tmp_path):
    """Test running CSPP."""
    import cspp_runner.runner
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))

    working_dir = tmp_path / "env"
    working_dir.mkdir(parents=True)
    fake_rdr_file = tmp_path / "RNSCA-RVIRS_j02_d20230511_t0016580_e0018234_b02578_c20230511001837310000_drlu_ops.h5"
    fake_rdr_file.touch()

    cspp_runner.runner.run_cspp(working_dir, "true", [], fake_rdr_file)


@patch('posttroll.message.Message')
def test_spawn_cspp_nominal(message, tmp_path, caplog,
                            fake_result_names, fakemessage, fake_empty_viirs_sdr_files,
                            monkeypatch):
    """Test spawning CSPP successfully."""
    from cspp_runner.runner import ViirsSdrProcessor

    message.return_value = 'my fake dummy message'
    fake_publisher = FakePublisher()

    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))
    (tmp_path / "env").mkdir(parents=True)

    def fake_run_cspp(call, args, *rdrs):
        p = tmp_path / "working_dir"
        p.mkdir()
        for f in fake_result_names:
            (p / f.name).touch()
        return os.fspath(p)

    with patch("cspp_runner.runner.ThreadPool"), \
            patch("cspp_runner.post_cspp.get_sdr_files") as get_sdr_files:
        vsp = ViirsSdrProcessor(1, tmp_path / "outdir", 'fake_topic')
        vsp.message_data = fakemessage.data

        get_sdr_files.return_value = fake_empty_viirs_sdr_files

        with patch("cspp_runner.runner.run_cspp") as crr:
            crr.side_effect = fake_run_cspp
            # fake_workingdir = fake_empty_viirs_sdr_files[0].parent
            vsp.working_dir = fake_empty_viirs_sdr_files[0].parent
            with caplog.at_level(logging.DEBUG):
                res_files = vsp.spawn_cspp(
                    pathlib.Path(os.fspath(
                        tmp_path /
                        "RNSCA-RVIRS_npp_d20230510_t1430538_e1432180_b59761_c20230512143418235765_drlu_ops.h5")),
                    # "RNSCA-RVIRS_j01_d20211229_t1342527_e1355397_b21199_c20211229144433345000_all-_dev.h5"),
                    # fake_workingdir,
                    publisher=fake_publisher,
                    viirs_sdr_call="touch",
                    viirs_sdr_options=[],
                    granule_time_tolerance=10)

    assert "Start CSPP" in caplog.text
    assert "CSPP probably failed" not in caplog.text
    assert "CSPP SDR processing finished..." in caplog.text
    assert "Number of SDR results files" in caplog.text
    assert len(res_files) == 28


def test_spawn_cspp_failure(monkeypatch, fakemessage, tmp_path, caplog):
    """Test spawning CSPP unsuccessfully."""
    from cspp_runner.runner import ViirsSdrProcessor
    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(tmp_path / "env"))
    (tmp_path / "env").mkdir(parents=True)

    fake_publisher = FakePublisher()

    with patch("cspp_runner.runner.ThreadPool"):
        vsp = ViirsSdrProcessor(1, tmp_path / "outdir", 'fake_topic')
        vsp.message_data = fakemessage.data

        fake_rdr_file = tmp_path / "RNSCA-RVIRS_j02_d20230511_t0016580_e0018234_b02578_c20230511001837310000_drlu_ops.h5"  # noqa
        fake_rdr_file.touch()
        fake_working_dir = tmp_path / 'work_dir'
        fake_working_dir.mkdir()
        vsp.working_dir = fake_working_dir

        with caplog.at_level(logging.WARNING):
            rf = vsp.spawn_cspp(fake_rdr_file,
                                # fake_working_dir,
                                publisher=fake_publisher,
                                viirs_sdr_call="false",
                                viirs_sdr_options=[])

    assert len(rf) == 0
    assert "CSPP probably failed!" in caplog.text


fake_publisher_config_contents = """name: test-publisher
port: 0
nameservers:
  - localhost
"""


class FakeCSPPAncillaryDataUpdater():
    """Fake CSPPAncillaryDataUpdater class."""

    def update_luts(self):
        """Fake update LUTs."""
        return

    def update_ancillary_data(self):
        """Fake update the ancillary data."""
        return


@patch('cspp_runner.cspp_utils.CSPPAncillaryDataUpdater')
def test_rolling_runner(fake_updater, tmp_path, caplog, monkeypatch, fakemessage,
                        fake_results):
    """Test NPP rolling runner."""
    from cspp_runner.runner import npp_rolling_runner

    fake_updater.return_value = FakeCSPPAncillaryDataUpdater()

    class TimeOut(Exception):
        pass

    def handler(signum, frame):
        raise TimeOut()

    fake_workdir = tmp_path / "workdir"
    fake_workdir.mkdir(exist_ok=True, parents=True)
    fake_updater.cspp_workdir = fake_workdir

    yaml_conf = tmp_path / "publisher.yaml"
    with yaml_conf.open(mode="wt", encoding="ascii") as fp:
        fp.write(fake_publisher_config_contents)

    monkeypatch.setenv("CSPP_WORKDIR", os.fspath(fake_workdir))
    with patch("posttroll.subscriber.Subscribe") as psS, \
            patch("cspp_runner.runner.Publish"), \
            patch("cspp_runner.runner.ViirsSdrProcessor.spawn_cspp") as spawn_cspp, \
            caplog.at_level(logging.DEBUG):

        psS.return_value.__enter__.return_value.recv.return_value = [fakemessage]
        spawn_cspp.return_value = fake_results

        try:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(2)
            npp_rolling_runner(7, 24,
                               os.fspath(tmp_path / "stamp_lut"),
                               os.fspath(tmp_path / "lut"),
                               "gopher://example.org/luts", "true",
                               "gopher://example.org/ancs",
                               os.fspath(tmp_path / "stamp_anc"),
                               "true", "/subscribe/file/available/rdr",
                               "/product/available/sdr",
                               tmp_path / "sdr_results",
                               "true", [],
                               granule_time_tolerance=10,
                               ncpus=2,
                               publisher_config=os.fspath(yaml_conf))
        except TimeOut:
            pass  # probably all is fine
        else:
            raise AssertionError()  # should never get here

    assert "Seconds since granule start:" in caplog.text
    assert "Get the results from the multiprocessing pool-run" in caplog.text
    assert "Now that SDR processing has completed, check for new LUT files..." in caplog.text
