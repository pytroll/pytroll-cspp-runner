"""Various helper functions for re-organizing the CSPP results after processing.

Scanning the CSPP working directory and cleaning up after CSPP processing
and move the SDR granules to a destination directory.
"""

import os
import stat
from datetime import datetime, timedelta
import shutil
from glob import glob
from trollsift import Parser
from cspp_runner.orbitno import TBUS_STYLE
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


VIIRS_SDR_FILE_PATTERN = '{dataset}_{platform_shortname}_d{start_time:%Y%m%d_t%H%M%S}{msec_start}_e{end_time:%H%M%S}{msec_end}_b{orbit:5d}_c{creation_time:%Y%m%d%H%M%S%f}_{source}.h5'  # noqa

EXPECTED_NUMBER_OF_SDR_FILES = 28


def cleanup_cspp_workdir(workdir):
    """Clean up the CSPP working dir after processing."""
    filelist = glob('%s/*' % workdir)
    for s in filelist:
        if os.path.isfile(s):
            os.remove(s)
    filelist = glob('%s/*' % workdir)
    LOG.info(
        "Number of items left after cleaning working dir = " + str(len(filelist)))
    shutil.rmtree(workdir)
    # os.mkdir(workdir)
    return


def get_ivcdb_files(sdr_dir):
    """Locate the ivcdb files needed for the VIIRS Active Fires algorithm.

    Please observe: These files are not part of the standard output of CSPP
    version 3.1 and earlier. Use '-d' flag and locate the files in
    sub-directories.
    """
    # From the Active Fires Insuidetallation G:
    # find . -type f -name 'IVCDB*.h5' -exec mv {} ${PWD} \;
    import fnmatch
    import os

    matches = []
    for root, _, filenames in os.walk(sdr_dir):
        for filename in fnmatch.filter(filenames, 'IVCDB*.h5'):
            matches.append(os.path.join(root, filename))

    return matches


def get_sdr_files(sdr_dir, **kwargs):
    """Get the sdr filenames (all M- and I-bands plus geolocation) for the direct readout swath."""
    params = {}
    params.update({'source': 'cspp_dev'})
    params.update(kwargs)
    # params.update({'start_time': kwargs.get('start_time')})
    if 'start_time' in params and not params.get('start_time'):
        params.pop('start_time')
    params.update({'platform_short_name': PLATFORM_NAME.get(kwargs.get('platform_name'))})
    try:
        params.pop('platform_name')
    except KeyError:
        pass

    time_tolerance = kwargs.get('time_tolerance', timedelta(seconds=0))
    if 'start_time' not in params or time_tolerance.total_seconds() == 0:
        sdr_files = get_sdr_filenames_from_pattern_and_parameters(sdr_dir, params)
        if len(sdr_files) < EXPECTED_NUMBER_OF_SDR_FILES:
            LOG.error("No or not enough SDR files found matching the RDR granule: Files found = %s",
                      str(sdr_files))
        return sdr_files

    sdr_files = get_sdr_filenames_from_pattern_and_parameters(sdr_dir, params)
    nfiles_found = len(sdr_files)
    if nfiles_found >= EXPECTED_NUMBER_OF_SDR_FILES:
        return sdr_files

    start_time = params['start_time']
    LOG.warning("No or not enough SDR files found matching the RDR granule: Files found = %d", nfiles_found)
    LOG.info("Will look for SDR files with a start time close in time to the start time of the RDR granule.")
    expected_start_time = start_time - time_tolerance
    sdr_files = []
    while nfiles_found < EXPECTED_NUMBER_OF_SDR_FILES and expected_start_time < start_time + time_tolerance:
        params.update({'start_time': expected_start_time})
        sdr_files = sdr_files + get_sdr_filenames_from_pattern_and_parameters(sdr_dir, params)
        nfiles_found = len(sdr_files)
        expected_start_time = expected_start_time + timedelta(seconds=1)

    # FIXME: Check for sufficient files an possibly raise an exception if not successful.
    if nfiles_found == EXPECTED_NUMBER_OF_SDR_FILES:
        LOG.debug("Expected number of SDR files found matching the RDR file.")
    else:
        LOG.error("Not enough SDR files found for the RDR scene: Files found = %d - Expected = %d",
                  nfiles_found, EXPECTED_NUMBER_OF_SDR_FILES)

    return sdr_files


def get_sdr_filenames_from_pattern_and_parameters(sdr_dir, params):
    """From a list of file pattern inputs get the list of file names by globbing in a directory."""
    p__ = Parser(VIIRS_SDR_FILE_PATTERN)

    params.update({'dataset': 'SVM??'})
    mband_files = [f for f in sdr_dir.glob(p__.globify(params))]
    params.update({'dataset': 'GM??O'})
    mband_files = mband_files + [f for f in sdr_dir.glob(p__.globify(params))]

    params.update({'dataset': 'SVI??'})
    iband_files = [f for f in sdr_dir.glob(p__.globify(params))]
    params.update({'dataset': 'GI??O'})
    iband_files = iband_files + [f for f in sdr_dir.glob(p__.globify(params))]

    params.update({'dataset': 'SVDNB'})
    dnb_files = [f for f in sdr_dir.glob(p__.globify(params))]
    params.update({'dataset': 'GDNBO'})
    dnb_files = dnb_files + [f for f in sdr_dir.glob(p__.globify(params))]

    params.update({'dataset': 'IVCDB'})
    ivcdb_files = [f for f in sdr_dir.glob(p__.globify(params))]

    return sorted(mband_files) + sorted(iband_files) + sorted(dnb_files) + sorted(ivcdb_files)


def create_subdirname(obstime, with_seconds=False, **kwargs):
    """Generate the pps subdirectory name from the start observation time.

    For example:
       'npp_20120405_0037_02270'
    """
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

    if with_seconds:
        return platform_name + obstime.strftime('_%Y%m%d_%H%M%S_') + '%.5d' % orbnum
    else:
        return platform_name + obstime.strftime('_%Y%m%d_%H%M_') + '%.5d' % orbnum


# def pack_sdr_files(sdrfiles, base_dir, subdir):
def pack_sdr_files(sdrfiles, dest_path):
    """Copy the SDR files to the destination under the *subdir* directory structure."""
    # path = pathlib.Path(base_dir) / subdir
    dest_path.mkdir(exist_ok=True, parents=True)

    LOG.info("Number of SDR files: " + str(len(sdrfiles)))
    retvl = []
    for sdrfile in sdrfiles:
        newfilename = dest_path / os.path.basename(sdrfile)
        LOG.info(f"Copy sdrfile to destination: {newfilename!s}")
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

        retvl.append(os.fspath(newfilename))

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
