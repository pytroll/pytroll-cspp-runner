"""Scanning the CSPP working directory and cleaning up after CSPP processing
and move the SDR granules to a destination directory"""

import os
import stat
from datetime import datetime
import shutil
from glob import glob
from cspp_runner.orbitno import TBUS_STYLE
from trollsift.parser import compose
import logging
LOG = logging.getLogger(__name__)

TLE_SATNAME = {'npp': 'SUOMI NPP',
               'j01': 'NOAA-20',
               'noaa20': 'NOAA-20',
               'noaa21': 'NOAA-20'
               }

PLATFORM_NAME = {'Suomi-NPP': 'npp',
                 'JPSS-1': 'noaa20',
                 'NOAA-20': 'noaa20',
                 'NOAA-21': 'noaa21'}


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


def get_ivcdb_files(sdr_dir):
    """Locate the ivcdb files need for the VIIRS Active Fires algorithm. These
       files are not yet part of the standard output of CSPP versio 3.1 and
       earlier. Use '-d' flag and locate the files in sub-directories

    """
    # From the Active Fires Insuidetallation G:
    # find . -type f -name 'IVCDB*.h5' -exec mv {} ${PWD} \;

    import fnmatch
    import os

    matches = []
    for root, dirnames, filenames in os.walk(sdr_dir):
        for filename in fnmatch.filter(filenames, 'IVCDB*.h5'):
            matches.append(os.path.join(root, filename))

    return matches


def get_sdr_files(sdr_dir, sensor, **kwargs):
    """Get the sdr filenames (all M- and I-bands plus geolocation for the
    direct readout swath"""

    if sensor == "viirs":
        # VIIRS M-bands + geolocation:
        mband_files = (glob(os.path.join(sdr_dir, 'SVM??_???_*.h5')) +
                       glob(os.path.join(sdr_dir, 'GM??O_???_*.h5')))
        # VIIRS I-bands + geolocation:
        iband_files = (glob(os.path.join(sdr_dir, 'SVI??_???_*.h5')) +
                       glob(os.path.join(sdr_dir, 'GI??O_???_*.h5')))
        # VIIRS DNB band + geolocation:
        dnb_files = (glob(os.path.join(sdr_dir, 'SVDNB_???_*.h5')) +
                     glob(os.path.join(sdr_dir, 'GDNBO_???_*.h5')))

        ivcdb_files = get_ivcdb_files(sdr_dir)

        return sorted(mband_files) + sorted(iband_files) + sorted(dnb_files) + sorted(ivcdb_files)

    elif sensor == "atms":
        # ATMS Brightness Temperature (SDR) and geolocation
        satms_files = (glob(os.path.join(sdr_dir, 'SATMS_???_*.h5')) +
                       glob(os.path.join(sdr_dir, 'GATMO_???_*.h5')))
        # ATMS Antenna Temperature (TDR) (are they needed ?)
        # atms_files = glob(os.path.join(sdr_dir, 'TATMS_???_*.h5')
        return satms_files

    elif sensor == "cris":
        # CRI[SF] (SDR) and geolocation (F is full resolution)
        scris_files = (glob(os.path.join(sdr_dir, 'SCRI[SF]_???_*.h5')) +
                       glob(os.path.join(sdr_dir, 'GCRSO_???_*.h5')))
        return scris_files

    else:
        LOG.error("Unknow sensor '%s'", sensor)


def create_subdirname(obstime, with_seconds=False, **kwargs):
    """Generate the pps subdirectory name from the start observation time, ex.:
    'npp_20120405_0037_02270'"""
    sat = kwargs.get('platform_name', 'npp')
    platform_name = PLATFORM_NAME.get(sat, sat)

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

    if 'subdir' in kwargs:
        subdir_compose = kwargs.copy()
        subdir_compose['platform_name'] = platform_name
        subdir_compose['orbnum'] = orbnum
        subdir_compose['start_time'] = obstime
        LOG.debug("subdir_compose_ {}".format(subdir_compose))
        subdir = compose(kwargs['subdir'], subdir_compose)
    else:
        if with_seconds:
            subdir = platform_name + obstime.strftime('_%Y%m%d_%H%M%S_') + '%.5d' % orbnum
        else:
            subdir = platform_name + obstime.strftime('_%Y%m%d_%H%M_') + '%.5d' % orbnum

    return subdir

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
        os.makedirs(path)

    LOG.info("Number of SDR files: " + str(len(sdrfiles)))
    retvl = []
    for sdrfile in sdrfiles:
        newfilename = os.path.join(path, os.path.basename(sdrfile))
        LOG.info("Copy sdrfile to destination: " + newfilename)
        if os.path.exists(sdrfile):
            LOG.info("File to copy: {file} <> ST_MTIME={time}".format(
                file=str(sdrfile),
                time=datetime.utcfromtimestamp(
                    os.stat(sdrfile)[stat.ST_MTIME]).strftime('%Y%m%d-%H%M%S')))
        shutil.copy(sdrfile, newfilename)
        if os.path.exists(newfilename):
            LOG.info("File at destination: {file} <> ST_MTIME={time}".format(
                file=str(newfilename),
                time=datetime.utcfromtimestamp(os.stat(newfilename)[stat.ST_MTIME]).strftime('%Y%m%d-%H%M%S')))

        retvl.append(newfilename)

    return retvl


# --------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage %s <cspp work dir>" % sys.argv[0])
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
