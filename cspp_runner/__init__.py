#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>
#   Lars Ørum Rasmussen <ras@dmi.dk>

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

"""
"""
import os
from datetime import datetime, timedelta
import re

import logging
LOG = logging.getLogger(__name__)

_RE_NPP_STAMP = re.compile(
    r'.*?(([A-Za-z0-9]+)_d(\d+)_t(\d+)_e(\d+)_b(\d+)).*')


class NPPStamp(object):

    """ A NPP stamp is:
    <platform>_d<start_date>_t<start_time>_e<end_time>_b<orbit_number>
    """

    def __init__(self, platform, start_time, end_time, orbit_number):
        self.platform = platform
        self.start_time = start_time
        self.end_time = end_time
        self.orbit_number = orbit_number

    def __str__(self):
        date = self.start_time.strftime('%Y%m%d')
        start = (self.start_time.strftime('%H%M%S') +
                 str(self.start_time.microsecond / 100000)[0])
        end = (self.end_time.strftime('%H%M%S') +
               str(self.end_time.microsecond / 100000)[0])
        return "%s_d%s_t%s_e%s_b%05d" % (self.platform, date, start, end,
                                         self.orbit_number)

    def __cmp__(self, other):
        return cmp(str(self), str(other))


def get_npp_stamp(filename):
    """A unique stamp for a granule.
    <name>_d<date>_t<start-time>_e<end-time>_b<orbit_number>
    """
    match = _RE_NPP_STAMP.match(os.path.basename(filename))
    if not match:
        return
    start_time, end_time = _dte2time(match.group(3), match.group(4),
                                     match.group(5))
    return NPPStamp(match.group(2), start_time, end_time,
                    int(match.group(6)))


def _dte2time(date, start_time, end_time):
    start_time = (datetime.strptime(date + start_time[:6], '%Y%m%d%H%M%S') +
                  timedelta(microseconds=int(start_time[6]) * 100000))
    end_time = (datetime.strptime(date + end_time[:6], '%Y%m%d%H%M%S') +
                timedelta(microseconds=int(end_time[6]) * 100000))
    if start_time > end_time:
        end_time += timedelta(days=1)
    return start_time, end_time


def get_datetime_from_filename(filename):
    """Get start observation time from the filename. Example:
    'GMODO_npp_d20120405_t0037099_e0038341_b00001_c20120405124731856767_cspp_dev.h5'
    """

    bname = os.path.basename(filename)
    sll = bname.split('_')
    return datetime.strptime(sll[2] + sll[3][:-1],
                             "d%Y%m%dt%H%M%S")
