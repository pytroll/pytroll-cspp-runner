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

"""Tests for main cmdline script."""

import os

from unittest.mock import patch


fake_conf_contents = """[test]
subscribe_topics = mars, venus
url_jpss_remote_lut_dir = https://example.org/luts
url_jpss_remote_anc_dir = https://example.org/ancs
lut_update_stampfile_prefix = stamp_lut
anc_update_stampfile_prefix = stamp_anc
url_download_trial_frequency_hours = 1
viirs_sdr_call = true
viirs_sdr_options = ['abc']
mirror_jpss_luts = true
mirror_jpss_ancillary = true
mode = unittest
level1_home = /nowhere/special
"""

fake_publisher_config_contents = """name: test-publisher
port: 0
nameservers:
  - localhost
"""


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    """Test parsing argumentsr."""
    import cspp_runner.viirs_dr_runner
    cspp_runner.viirs_dr_runner.parse_args()
    assert ap.return_value.add_argument.call_count == 4


@patch("cspp_runner.viirs_dr_runner.parse_args")
@patch("cspp_runner.viirs_dr_runner.npp_rolling_runner")
def test_main(crn, cvp, tmp_path):
    import cspp_runner.viirs_dr_runner

    # create fake config files
    conf = tmp_path / "conf.ini"
    with conf.open(mode="wt", encoding="ascii") as fp:
        fp.write(fake_conf_contents)
    yaml_conf = tmp_path / "publisher.yaml"
    with yaml_conf.open(mode="wt", encoding="ascii") as fp:
        fp.write(fake_publisher_config_contents)

    # fake log destination
    log = tmp_path / "log"

    # fake

    cvp.return_value = cspp_runner.viirs_dr_runner.get_parser().parse_args(
            ["-c", os.fspath(conf),
             "-C", "test",
             "-l", os.fspath(log),
             "-p", os.fspath(yaml_conf)])

    cspp_runner.viirs_dr_runner.main()
