"""Scanning the CSPP working directory and cleaning up after CSPP processing
and move the SDR granules to a destination directory"""

import os
import stat
from datetime import datetime
import shutil
from glob import glob
from cspp_runner.orbitno import TBUS_STYLE
import logging
LOG = logging.getLogger(__name__)

TLE_SATNAME = {'npp': 'SUOMI NPP',
               'j01': 'JPSS-1',
               'noaa20': 'JPSS-1'
               }


def cleanup_cspp_workdir(workdir):
    """Clean up the CSPP working dir after processing"""

    filelist = glob('%s/*' % workdir)
    dummy = [os.remove(s) for s in filelist if os.path.isfile(s)]
    filelist = glob('%s/*' % workdir)
    LOG.info(
        "Number of items left after cleaning working dir = " + str(len(filelist)))
    shutil.rmtree(workdir)
    # os.mkdir(workdir)
    return


def get_sdr_files(sdr_dir, **kwargs):
    """Get the sdr filenames (all M- and I-bands plus geolocation for the
    direct readout swath"""

    # VIIRS M-bands + geolocation:
    mband_files = (glob(os.path.join(sdr_dir, 'SVM??_???_*.h5')) +
                   glob(os.path.join(sdr_dir, 'GM??O_???_*.h5')))
    # VIIRS I-bands + geolocation:
    iband_files = (glob(os.path.join(sdr_dir, 'SVI??_???_*.h5')) +
                   glob(os.path.join(sdr_dir, 'GI??O_???_*.h5')))
    # VIIRS DNB band + geolocation:
    dnb_files = (glob(os.path.join(sdr_dir, 'SVDNB_???_*.h5')) +
                 glob(os.path.join(sdr_dir, 'GDNBO_???_*.h5')))

    return sorted(mband_files) + sorted(iband_files) + sorted(dnb_files)


def create_subdirname(obstime, with_seconds=False, **kwargs):
    """Generate the pps subdirectory name from the start observation time, ex.:
    'npp_20120405_0037_02270'"""
    platform_name = kwargs.get(platform_name, 'npp')

    if "orbit" in kwargs:
        orbnum = int(kwargs['orbit'])
    else:
        from pyorbital.orbital import Orbital
        from cspp_runner import orbitno

        try:
            tle = orbitno.get_tle(TLE_SATNAME.get(platform_name), obstime)
            orbital_ = Orbital(tle.platform, line1=tle.line1, line2=tle.line2)
            orbnum = orbital_.get_orbit_number(obstime, tbus_style=TBUS_STYLE)
        except orbitno.NoTleFile:
            LOG.error('Not able to determine orbit number!')
            import traceback
            traceback.print_exc(file=sys.stderr)
            orbnum = 1

    if with_seconds:
        return platform_name + obstime.strftime('_%Y%m%d_%H%M%S_') + '%.5d' % orbnum
    else:
        return platform_name + obstime.strftime('_%Y%m%d_%H%M_') + '%.5d' % orbnum


def make_okay_files(base_dir, subdir_name):
    """Make okay file to signal that all SDR files have been placed in
    destination directory"""
    import subprocess
    okfile = os.path.join(base_dir, subdir_name + ".okay")
    subprocess.call(['touch', okfile])
    return


def pack_sdr_files(sdrfiles, base_dir, subdir):
    """Copy the SDR files to the sub-directory under the *subdir* directory
    structure"""

    path = os.path.join(base_dir, subdir)
    if not os.path.exists(path):
        os.mkdir(path)

    LOG.info("Number of SDR files: " + str(len(sdrfiles)))
    retvl = []
    for sdrfile in sdrfiles:
        newfilename = os.path.join(path, os.path.basename(sdrfile))
        LOG.info("Copy sdrfile to destination: " + newfilename)
        if os.path.exists(sdrfile):
            LOG.info("File to copy: {file} <> ST_MTIME={time}".format(file=str(sdrfile),
                                                                      time=datetime.utcfromtimestamp(os.stat(sdrfile)[stat.ST_MTIME]).strftime('%Y%m%d-%H%M%S')))
        shutil.copy(sdrfile, newfilename)
        if os.path.exists(newfilename):
            LOG.info("File at destination: {file} <> ST_MTIME={time}".format(file=str(newfilename),
                                                                             time=datetime.utcfromtimestamp(os.stat(newfilename)[stat.ST_MTIME]).strftime('%Y%m%d-%H%M%S')))

        retvl.append(newfilename)

    return retvl

# --------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print "Usage %s <cspp work dir>" % sys.argv[0]
        sys.exit()
    else:
        # SDR DIR:
        CSPP_WRKDIR = sys.argv[1]

    rootdir = "/san1/pps/import/PPS_data/source"
    from cspp_runner import get_datetime_from_filename
    FILES = get_sdr_files(CSPP_WRKDIR)
    start_time = get_datetime_from_filename(FILES[0])

    subd = create_subdirname(start_time)
    pack_sdr_files(FILES, rootdir, subd)
    make_okay_files(rootdir, subd)
