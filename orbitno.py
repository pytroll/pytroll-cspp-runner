import os
from datetime import datetime, timedelta
import re

from pyorbital.orbital import Orbital
from npp_runner import get_npp_stamp

import logging
LOG = logging.getLogger(__name__)


class NoTleFile(Exception):
    pass

TLE_SATNAME = {'npp': 'SUOMI NPP', }
TBUS_STYLE = False

_re_replace_orbitno = re.compile(r"_b(\d{5})")


def replace_orbitno(filename):
    stamp = get_npp_stamp(filename)

    # Correct h5 attributes
    no_date = datetime(1958, 1, 1)
    epsilon_time = timedelta(days=2)
    import h5py

    def _get_a_good_time(name, obj):
        del name
        if isinstance(obj, h5py.Dataset):
            date_key, time_key = ('Ending_Date', 'Ending_Time')
            if date_key in obj.attrs.keys():
                if not good_time_val_[0]:
                    time_val = datetime.strptime(
                        obj.attrs[date_key][0][0] +
                        obj.attrs[time_key][0][0],
                        '%Y%m%d%H%M%S.%fZ')
                    if abs(time_val - no_date) > epsilon_time:
                        good_time_val_[0] = time_val

    def _check_orbitno(name, obj):
        del name
        if isinstance(obj, h5py.Dataset):
            for date_key, time_key, orbit_key in (
                ('AggregateBeginningDate', 'AggregateBeginningTime',
                 'AggregateBeginningOrbitNumber'),
                ('AggregateEndingDate', 'AggregateEndingTime',
                 'AggregateEndingOrbitNumber'),
                ('Beginning_Date', 'Beginning_Time',
                 'N_Beginning_Orbit_Number')):
                if orbit_key in obj.attrs.keys():
                    time_val = datetime.strptime(
                        obj.attrs[date_key][0][0] +
                        obj.attrs[time_key][0][0],
                        '%Y%m%d%H%M%S.%fZ')

                    # Check for no date (1958) problem:
                    if abs(time_val - no_date) < epsilon_time:
                        LOG.info("Start time wrong: %s",
                                 time_val.strftime('%Y%m%d'))
                        LOG.info("Will use the first good end time encounter " +
                                 "in file to determine orbit number")
                        time_val = good_time_val_[0]

                    orbit_val = orbital_.get_orbit_number(time_val,
                                                          tbus_style=TBUS_STYLE)
                    obj.attrs.modify(orbit_key, [[orbit_val]])
                    counter_[0] += 1

    # Correct h5 attributes
    orbital_ = Orbital(TLE_SATNAME[stamp.platform])
    orbit = orbital_.get_orbit_number(stamp.start_time, tbus_style=TBUS_STYLE)
    LOG.info("Replacing orbit number %05d with %05d",
             stamp.orbit_number, orbit)
    fp = h5py.File(filename, 'r+')
    try:
        good_time_val_ = [None]
        fp.visititems(_get_a_good_time)
        counter_ = [0]
        fp.visititems(_check_orbitno)
        if counter_[0] == 0:
            raise IOError(
                "Failed replacing orbit number in hdf5 attributes '%s'" % filename)
        LOG.info("Replaced orbit number in %d attributes", counter_[0])
    finally:
        fp.close()

    # Correct filename
    dname, fname = os.path.split(filename)
    fname, n = _re_replace_orbitno.subn('_b%05d' % orbit, fname)
    if n != 1:
        raise IOError("Failed replacing orbit number in filename '%s'" % fname)
    return os.path.join(dname, fname), orbit

if __name__ == '__main__':
    import sys
    print replace_orbitno(sys.argv[1])
