#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2020 Pytroll

# Author(s):

#   Martin.Raspaud <martin.raspaud@smhi.se>

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

from cspp_runner import get_sdr_times, get_datetime_from_filename
from datetime import datetime
from unittest import TestCase


class TestTimeExtraction(TestCase):

    def test_get_sdr_times(self):
        filename = 'GMODO_npp_d20120405_t0037099_e0038341_b00001_c20120405124731856767_cspp_dev.h5'
        start_time, end_time = get_sdr_times(filename)
        assert start_time == datetime(2012, 4, 5, 0, 37, 9, 900000)
        assert end_time == datetime(2012, 4, 5, 0, 38, 34, 100000)

    def test_get_sdr_times_over_midnight(self):
        filename = 'GMODO_npp_d20120405_t2359099_e0000341_b00001_c20120405124731856767_cspp_dev.h5'
        start_time, end_time = get_sdr_times(filename)
        assert start_time == datetime(2012, 4, 5, 23, 59, 9, 900000)
        assert end_time == datetime(2012, 4, 6, 0, 0, 34, 100000)
        assert get_datetime_from_filename(filename) == start_time
