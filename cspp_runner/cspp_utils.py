#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Pytroll Developers

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

"""Utilities and helper functions supporting the CSPP processing runners."""

import os
import netifaces
import logging
import pathlib
import shutil
import stat
import subprocess
from datetime import datetime, timedelta
from glob import glob

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

_NPP_SDRPROC_LOG_FILE = os.environ.get('NPP_SDRPROC_LOG_FILE', None)

LOG = logging.getLogger(__name__)


class CSPPAncillaryDataUpdater():
    """A class to handle the update of CSPP dynamic ancillary data and LUT files."""

    def __init__(self, **kwargs):
        """Initialize the CSPP ancillary and LUTs data updater."""
        self.url_download_trial_frequency_hours = kwargs.get('url_download_trial_frequency_hours')

        self.thr_lut_files_age_days = kwargs.get('thr_lut_files_age_days')
        self.lut_update_stampfile_prefix = kwargs.get('lut_update_stampfile_prefix')
        self.lut_dir = kwargs.get('lut_dir')
        self.mirror_jpss_luts = kwargs.get('mirror_jpss_luts')
        self.url_jpss_remote_lut_dir = kwargs.get('url_jpss_remote_lut_dir')

        self.url_jpss_remote_anc_dir = kwargs.get('url_jpss_remote_anc_dir')
        self.anc_update_stampfile_prefix = kwargs.get('anc_update_stampfile_prefix')
        self.mirror_jpss_ancillary = kwargs.get('mirror_jpss_ancillary')

        self.initalize_working_dir()

    def initalize_working_dir(self):
        """Check and set the CSPP working directory."""
        _check_environment("CSPP_WORKDIR")
        self.cspp_workdir = os.environ.get("CSPP_WORKDIR", '')
        pathlib.Path(self.cspp_workdir).mkdir(parents=True, exist_ok=True)

    def update_luts(self):
        """Update the LUT files as needed."""
        if not self.mirror_jpss_luts:
            LOG.debug("No LUT update script provided. No LUT updating will be attempted.")
            return

        fresh = self.check_lut_files()
        if fresh:
            LOG.info("Files in the LUT dir are fresh...")
            LOG.info("...or download has been attempted recently! No url downloading....")
            return

        LOG.warning("Files in the LUT dir are non existent or old. Start url fetch...")
        self._update_lut_files()

    def update_ancillary_data(self):
        """Update the Anciallary data files for CSPP if needed."""
        if not self.mirror_jpss_ancillary:
            LOG.debug("No ancillary data update script provided. CSPP ancillary data will not be updated.")
            return

        LOG.info("Dynamic ancillary data will be updated. Start url fetch...")
        self._update_ancillary_files()

    def check_lut_files(self):
        """Check if LUT files are present and fresh.

        Check if the LUT files under ${path_to_cspp_cersion}/anc/cache/luts are
        available and check if they are fresh. Return True if fresh/new files
        exists, otherwise False.  It is files like these (with incredible
        user-unfriendly) names: 510fc93d-8e4ed-6880946f-f2cdb929.asc
        510fc93d-8e4ed-6880946f-f2cdb929.VIIRS-SDR-GAIN-LUT_npp_20120217000000Z_20120220000001Z_ee00000000000000Z_PS-1-N-CCR-12-330-JPSS-DPA-002-PE-_noaa_all_all-_all.bin
        etc...

        We do not yet know if these files are always having the same name or if
        the number of files are expected to always be the same!?  Thus
        searching and checking is a bit difficult. We check if there are any
        files at all, and then how old the latest file is, and hope that this
        is sufficient.

        """  # noqa
        now = datetime.utcnow()
        tdelta = timedelta(
            seconds=float(self.url_download_trial_frequency_hours) * 3600.)
        # Get the time of the last update trial:
        files = glob(str(self.lut_update_stampfile_prefix) + '*')
        # Can we count on glob sorting the most recent file first. In case we can,
        # we don't need to check the full history of time stamp files. This will
        # save time! Currently we check all files...
        # FIXME!
        update_it = True
        for filename in files:
            tslot = datetime.strptime(
                os.path.basename(filename).split('.')[-1], '%Y%m%d%H%M')
            if now - tslot < tdelta:
                update_it = False
                break

        if not update_it:
            LOG.info('An URL update trial has been attempted recently. Continue')
            return True

        LOG.info('No update trial seems to have been attempted recently')
        tdelta = timedelta(days=int(self.thr_lut_files_age_days))

        files_ok = True
        LOG.info("Directory " + str(self.lut_dir) + "...")
        files = glob(os.path.join(self.lut_dir, '*'))
        if len(files) == 0:
            LOG.info("No LUT files available!")
            return False

        filename = files[0]
        tstamp = os.stat(filename)[stat.ST_MTIME]
        first_time = datetime.utcfromtimestamp(tstamp)

        if (now - first_time) > tdelta:
            LOG.info("File too old! File=%s " % filename)
            files_ok = False

        return files_ok

    def _update_lut_files(self, timeout=600):
        """Update LUT files for VIIRS processing.

        Updates the ancillary LUT files over internet.

        These files need to be updated at least once every week, in order to
        achieve the best possible SDR processing.

        """
        update_files(
            self.url_jpss_remote_lut_dir,
            self.lut_update_stampfile_prefix,
            self.mirror_jpss_luts,
            "LUT",
            timeout=timeout)

    def _update_ancillary_files(self, timeout=600):
        """Update the dynamic ancillary files needed for CSPP VIIRS SDR processing.

        These data files encompass Two Line Element (TLE) and Polar Wander (PW)
        files, and should preferably be updated daily. This is done
        automatically in CSPP if the viirs_sdr script is run without the '-l'
        option. However, to slightly speed up the processing and avoid hangups
        depending on internet connectivity this script can omit the automatic
        download (using the '-l' option) and thus the files need to be updated
        outside the script.

        """
        update_files(
            self.url_jpss_remote_anc_dir,
            self.anc_update_stampfile_prefix,
            self.mirror_jpss_ancillary,
            "ANC",
            timeout=timeout)


def update_files(url_jpss_remote_dir, update_stampfile_prefix, mirror_jpss,
                 what, timeout=600):
    """Do the update of the LUT files on disk.

    Runs the fetch over internet from a separate working directory, and calls
    the JPSS script in a separat shell.

    """
    _check_environment("CSPP_WORKDIR")
    cspp_workdir = os.environ.get("CSPP_WORKDIR", '')
    pathlib.Path(cspp_workdir).mkdir(parents=True, exist_ok=True)
    my_env = os.environ.copy()
    my_env['JPSS_REMOTE_ANC_DIR'] = url_jpss_remote_dir

    LOG.info(f"Start downloading {what:s}....")
    cmd = [shutil.which(mirror_jpss), "-W", cspp_workdir]
    LOG.info(f"Download command for {what:s}: {cmd!s}")

    proc = subprocess.Popen(
        cmd, shell=False, env=my_env,
        cwd=cspp_workdir,
        stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    while (line := proc.stdout.readline()):
        LOG.info(line.decode("utf-8").strip('\n'))
    while (line := proc.stderr.readline()):
        LOG.error(line.decode("utf-8").strip('\n'))

    try:
        returncode = proc.wait(timeout)
    except subprocess.TimeoutExpired:
        LOG.exception(f"Attempt to update {what:s} files timed out. ")

    if returncode != 0:
        LOG.exception(
            f"Attempt to update {what:s} files failed with exit code "
            f"{returncode:d}.")
    else:
        now = datetime.utcnow()
        timestamp = now.strftime('%Y%m%d%H%M')

        filename = update_stampfile_prefix + '.' + timestamp
        try:
            fpt = open(filename, "w")
            fpt.write(timestamp)
        except OSError:
            LOG.warning(f'Failed to write {what:s}-update time-stamp file')
            return
        else:
            fpt.close()

        LOG.info(f"{what:s} downloaded. {what:s}-update timestamp file = " + filename)


def _check_environment(*args):
    """Check that requested environment variables are set.

    Raise EnvironmentError if they are not.
    """
    missing = set()
    for arg in args:
        if arg not in os.environ:
            missing.add(arg)
    if missing:
        raise EnvironmentError("Missing environment variables: " +
                               ", ".join(missing))


def get_local_ips():
    """Get the local IP address of where CSPP is running."""
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips
