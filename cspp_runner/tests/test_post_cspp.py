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

"""Tests for post-cspp module."""

import logging
import os


def test_pack_sdr_files(tmp_path, caplog):
    from cspp_runner.post_cspp import pack_sdr_files

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
