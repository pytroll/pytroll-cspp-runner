#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c14526.ad.smhi.se>

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

"""An NPP RDR data ingester
"""

from posttroll.publisher import Publish
from posttroll.message import Message
import time
import os.path

import os
MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "dev"


files = [
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1355276_e1356477_b00001_c20141031135816944000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1356495_e1358131_b00001_c20141031135941994000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1358150_e1359385_b00001_c20141031140107539000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1359403_e1401040_b00001_c20141031140233092000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1401057_e1402293_b00001_c20141031140358156000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1402311_e1403547_b00001_c20141031140523701000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1403565_e1405201_b00001_c20141031140649827000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1405220_e1406455_b00001_c20141031140706058000_nfts_drl.h5",
    "/san1/polar_in/direct_readout/npp/lvl0/RNSCA-RVIRS_npp_d20141031_t1406473_e1406562_b00001_c20141031140706175000_nfts_drl.h5",
]


def get_rdr_times(filename):
    from datetime import datetime, timedelta

    bname = os.path.basename(filename)
    sll = bname.split('_')
    start_time = datetime.strptime(sll[2] + sll[3][:-1],
                                   "d%Y%m%dt%H%M%S")
    end_time = datetime.strptime(sll[2] + sll[4][:-1],
                                 "d%Y%m%de%H%M%S")
    if end_time < start_time:
        end_time += timedelta(days=1)
    return start_time, end_time


def create_rdr_message(filename):

    data = {}
    data["platform_name"] = "Suomi-NPP"
    data["format"] = "RDR"
    data["sensor"] = "viirs"
    data["type"] = "HDF5"
    data["data_processing_level"] = "0"
    data["orbit_number"] = "00001"
    data["start_time"], data["end_time"] = get_rdr_times(filename)
    data["filename"] = os.path.basename(filename)
    data["uri"] = "ssh://safe.smhi.se" + filename
    environment = MODE
    msg = Message('/' + data['format'] + '/' + data['data_processing_level'] +
                  '/norrköping/' + environment + '/polar/direct_readout/',
                  "file", data).encode()
    print "Publishing", msg
    return msg

try:
    with Publish("receiver", 0) as pub:
        for filename in files:
            message = create_rdr_message(filename)
            print "publishing", message
            pub.send(str(message))
            time.sleep(10)
            # time.sleep(85)

except KeyboardInterrupt:
    print "terminating publisher..."
