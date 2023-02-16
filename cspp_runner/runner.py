# Copyright (c) 2013 - 2021 pytroll-cspp-runner developers

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

"""Level-1 processing for VIIRS Suomi NPP Direct Readout data. Using the CSPP
level-1 processor from the SSEC, Wisconsin, based on the ADL from the NASA DRL.
Listen for pytroll messages from Nimbus (NPP file dispatch) and trigger
processing on direct readout RDR data (granules or full swaths)
"""


import os
import socket
import logging
import multiprocessing
import netifaces
import pathlib
import shutil
import stat
import subprocess
import tempfile
import time
import yaml
from glob import glob
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from urllib.parse import urlunsplit, urlparse

import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message

import cspp_runner
import cspp_runner.orbitno
from cspp_runner import (get_datetime_from_filename, get_sdr_times, is_same_granule)
from cspp_runner.post_cspp import (get_sdr_files,
                                   create_subdirname,
                                   pack_sdr_files, make_okay_files,
                                   cleanup_cspp_workdir)
from cspp_runner.pre_cspp import fix_rdrfile

PATH = os.environ.get('PATH', '')

CSPP_SDR_HOME = os.environ.get("CSPP_SDR_HOME", '')
CSPP_RT_SDR_LUTS = os.path.join(CSPP_SDR_HOME, 'anc/cache/incoming_luts')
APPL_HOME = os.environ.get('NPP_SDRPROC', '')

VIIRS_SATELLITES = ['Suomi-NPP', 'NOAA-20', 'NOAA-21']

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

_NPP_SDRPROC_LOG_FILE = os.environ.get('NPP_SDRPROC_LOG_FILE', None)

LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
def check_lut_files(thr_days, url_download_trial_frequency_hours,
                    lut_update_stampfile_prefix, lut_dir):
    """Check if the LUT files under ${path_to_cspp_cersion}/anc/cache/luts are
    available and check if they are fresh. Return True if fresh/new files
    exists, otherwise False.
    It is files like these (with incredible user-unfriendly) names:
    510fc93d-8e4ed-6880946f-f2cdb929.asc
    510fc93d-8e4ed-6880946f-f2cdb929.VIIRS-SDR-GAIN-LUT_npp_20120217000000Z_20120220000001Z_ee00000000000000Z_PS-1-N-CCR-12-330-JPSS-DPA-002-PE-_noaa_all_all-_all.bin
    etc...

    We do not yet know if these files are always having the same name or if the
    number of files are expected to always be the same!?  Thus searching and
    checking is a bit difficult. We check if there are any files at all, and then
    how old the latest file is, and hope that this is sufficient.

    """

    now = datetime.utcnow()

    tdelta = timedelta(
        seconds=float(url_download_trial_frequency_hours) * 3600.)
    # Get the time of the last update trial:
    files = glob(lut_update_stampfile_prefix + '*')
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
    tdelta = timedelta(days=int(thr_days))

    files_ok = True
    LOG.info("Directory " + str(lut_dir) + "...")
    files = glob(os.path.join(lut_dir, '*'))
    if len(files) == 0:
        LOG.info("No LUT files available!")
        return False

    filename = files[0]
    tstamp = os.stat(filename)[stat.ST_MTIME]
    first_time = datetime.utcfromtimestamp(tstamp)
    # tstamp = os.stat(files[-1])[stat.ST_MTIME]
    # last_time = datetime.utcfromtimestamp(tstamp)

    if (now - first_time) > tdelta:
        LOG.info("File too old! File=%s " % filename)
        files_ok = False

    return files_ok


def update_lut_files(url_jpss_remote_lut_dir,
                     lut_update_stampfile_prefix, mirror_jpss_luts,
                     timeout=600):
    """
    Function to update the ancillary LUT files

    These files need to be updated at least once every week, in order to
    achieve the best possible SDR processing.

    """

    update_files(
            url_jpss_remote_lut_dir,
            lut_update_stampfile_prefix,
            mirror_jpss_luts,
            "LUT",
            timeout=timeout)


def update_files(url_jpss_remote_dir, update_stampfile_prefix, mirror_jpss,
                 what,
                 timeout=600):
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
        except OSError as e:
            LOG.warning(
                f'Failed to write {what:s}-update time-stamp file to '
                f"{filename!s}: {e.args[1]:s}")
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


def update_ancillary_files(url_jpss_remote_anc_dir,
                           anc_update_stampfile_prefix, mirror_jpss_ancillary,
                           timeout=600):
    """
    Function to update the dynamic ancillary data.

    These data files encompass Two Line Element (TLE) and Polar Wander (PW)
    files, and should preferably be updated daily. This is done automatically
    in CSPP if the viirs_sdr script is run without the '-l' option. However, to
    slightly speed up the processing and avoid hangups depending on internet
    connectivity this script can omit the automatic download (using the '-l'
    option) and thus the files need to be updated outside the script.

    """
    update_files(
            url_jpss_remote_anc_dir,
            anc_update_stampfile_prefix,
            mirror_jpss_ancillary,
            "ANC",
            timeout=timeout)


def run_cspp(viirs_sdr_call, viirs_sdr_options, *viirs_rdr_files):
    """Run CSPP on VIIRS RDR files"""

    LOG.info("viirs_sdr_options = " + str(viirs_sdr_options))
    path = os.environ["PATH"]
    LOG.info("Path from environment: %s", str(path))
    if not isinstance(viirs_sdr_options, list):
        LOG.warning("No options will be passed to CSPP")
        viirs_sdr_options = []

    cspp_workdir = os.environ.get("CSPP_WORKDIR", '')
    try:
        working_dir = tempfile.mkdtemp(dir=cspp_workdir)
    except OSError:
        working_dir = tempfile.mkdtemp()

    # Run the command:
    cmdlist = [viirs_sdr_call]
    cmdlist.extend(viirs_sdr_options)
    cmdlist.extend(viirs_rdr_files)
    t0_clock = time.process_time()
    t0_wall = time.time()
    LOG.info("Popen call arguments: " + str(cmdlist))
    viirs_sdr_proc = subprocess.Popen(
            cmdlist, cwd=working_dir,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE)
    while True:
        line = viirs_sdr_proc.stdout.readline()
        if not line:
            break
        LOG.info(line.decode("utf-8").strip('\n'))

    while True:
        errline = viirs_sdr_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline.decode("utf-8").strip('\n'))

    LOG.info("Seconds process time: " + (str(time.process_time() - t0_clock)))
    LOG.info("Seconds wall clock time: " + (str(time.time() - t0_wall)))

    viirs_sdr_proc.poll()
    return working_dir


def publish_sdr(publisher, result_files, mda, site, mode,
                publish_topic, **kwargs):
    """Publish the messages that SDR files are ready
    """
    if not result_files:
        return

    # Now publish:
    to_send = mda.copy()
    # Delete the RDR uri and uid from the message:
    try:
        del to_send['uri']
    except KeyError:
        LOG.warning("Couldn't remove URI from message")
    try:
        del to_send['uid']
    except KeyError:
        LOG.warning("Couldn't remove UID from message")

    if 'orbit' in kwargs:
        to_send["orig_orbit_number"] = to_send["orbit_number"]
        to_send["orbit_number"] = kwargs['orbit']

    to_send["dataset"] = []
    start_times = set()
    end_times = set()
    for result_file in result_files:
        filename = os.path.basename(result_file)
        to_send[
            'dataset'].append({'uri': urlunsplit(('ssh', socket.gethostname(),
                                                  result_file, '', '')),
                               'uid': filename})
        (start_time, end_time) = get_sdr_times(filename)
        start_times.add(start_time)
        end_times.add(end_time)
    to_send['format'] = 'SDR'
    to_send['type'] = 'HDF5'
    to_send['data_processing_level'] = '1B'
    to_send['start_time'] = min(start_times)
    to_send['end_time'] = max(end_times)

    LOG.debug('Site = %s', site)
    LOG.debug('Publish topic = %s', publish_topic)
    msg = Message('/'.join(('',
                            publish_topic,
                            to_send['format'],
                            to_send['data_processing_level'],
                            site,
                            mode,
                            'polar',
                            'direct_readout')),
                  "dataset", to_send).encode()
    LOG.debug("sending: " + str(msg))
    publisher.send(msg)


def spawn_cspp(current_granule, *glist, viirs_sdr_call, viirs_sdr_options, **kwargs):
    """Spawn a CSPP run on the set of RDR files given"""

    start_time = kwargs.get('start_time')
    platform_name = kwargs.get('platform_name')

    LOG.info("Start CSPP: RDR files = " + str(glist))
    working_dir = run_cspp(viirs_sdr_call, viirs_sdr_options, *glist)
    LOG.info("CSPP SDR processing finished...")
    # Assume everything has gone well!
    new_result_files = get_sdr_files(working_dir, platform_name=platform_name)
    LOG.info("SDR file names: %s", str([os.path.basename(f) for f in new_result_files]))
    if len(new_result_files) == 0:
        LOG.warning("No SDR files available. CSPP probably failed!")
        return working_dir, []

    LOG.info("current_granule = " + str(current_granule))
    LOG.info("glist = " + str(glist))
    if current_granule in glist and len(glist) == 1:
        LOG.info("Current granule is identical to the 'list of granules'" +
                 " No sdr result files will be skipped")
        return working_dir, new_result_files

    # Only bother about the "current granule" - skip the rest
    if start_time:
        LOG.info("Start time of current granule (from messages): %s", start_time.strftime('%Y-%m-%d %H:%M'))

    start_time = get_datetime_from_filename(current_granule)
    LOG.info("Start time of current granule: %s", start_time.strftime('%Y-%m-%d %H:%M:%S'))
    sec_tolerance = int(kwargs.get('granule_time_tolerance', 10))
    LOG.info("Time tolerance to identify which SDR granule belong " +
             "to the RDR granule being processed: " + str(sec_tolerance))
    result_files = [new_file for new_file in new_result_files if is_same_granule(
        current_granule, new_file, sec_tolerance)]

    LOG.info("Number of results files = " + str(len(result_files)))
    return working_dir, result_files


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


class ViirsSdrProcessor:

    """
    Container for the VIIRS SDR processing based on CSPP

    """

    def __init__(self, ncpus, level1_home):
        self.pool = ThreadPool(ncpus)
        self.ncpus = ncpus

        self.orbit_number = 1  # Initialised orbit number
        self.platform_name = 'unknown'  # Ex.: Suomi-NPP
        self.fullswath = False
        self.cspp_results = []
        self.glist = []
        self.pass_start_time = None
        self.result_files = []
        self.sdr_home = level1_home
        self.message_data = None

    def initialise(self):
        """Initialise the processor"""
        self.fullswath = False
        self.cspp_results = []
        self.glist = []
        self.pass_start_time = None
        self.result_files = []

    def pack_sdr_files(self, subd):
        return pack_sdr_files(self.result_files, self.sdr_home, subd)

    def run(self, msg, viirs_sdr_call, viirs_sdr_options,
            granule_time_tolerance=10):
        """Start the VIIRS SDR processing using CSPP on one rdr granule"""

        if msg:
            LOG.debug("Received message: " + str(msg))

        LOG.debug("glist: " + str(self.glist))
        if msg is None and self.glist and len(self.glist) > 2:
            # The swath is assumed to be finished now
            LOG.debug("The swath is assumed to be finished now")
            del self.glist[0]
            keeper = self.glist[1]
            LOG.info("Start CSPP: RDR files = " + str(self.glist))
            self.cspp_results.append(
                    self.pool.apply_async(
                        spawn_cspp,
                        [keeper] + self.glist,
                        {"viirs_sdr_call": viirs_sdr_call,
                         "viirs_sdr_options": viirs_sdr_options,
                         "granule_time_tolerance": granule_time_tolerance}))
            LOG.debug("Inside run: Return with a False...")
            return False
        elif msg and ('platform_name' not in msg.data or 'sensor' not in msg.data):
            LOG.debug("No platform_name or sensor in message. Continue...")
            return True
        elif msg and not (msg.data['platform_name'] in VIIRS_SATELLITES and
                          'viirs' in msg.data['sensor']):
            LOG.info("Not a VIIRS scene. Continue...")
            return True
        elif msg is None:
            LOG.debug("Message is None. glist = %s", str(self.glist))
            return True

        LOG.debug("")
        LOG.debug("\tMessage:")
        LOG.debug(str(msg))
        urlobj = urlparse(msg.data['uri'])
        LOG.debug("Server = " + str(urlobj.netloc))
        url_ip = socket.gethostbyname(urlobj.netloc)
        if url_ip not in get_local_ips():
            LOG.warning(
                "Server %s not the current one: %s" % (str(urlobj.netloc),
                                                       socket.gethostname()))

        LOG.info("Ok... " + str(urlobj.netloc))
        LOG.info("Sat and Instrument: " + str(msg.data['platform_name']) +
                 " " + str(msg.data['sensor']))

        self.platform_name = str(msg.data['platform_name'])
        self.message_data = msg.data
        start_time = msg.data['start_time']
        try:
            end_time = msg.data['end_time']
        except KeyError:
            LOG.warning(
                "No end_time in message! Guessing start_time + 86 seconds...")
            end_time = msg.data['start_time'] + timedelta(seconds=86)
        try:
            orbnum = int(msg.data['orbit_number'])
        except KeyError:
            LOG.warning("No orbit_number in message! Set to none...")
            orbnum = None

        rdr_filename = urlobj.path
        dummy, fname = os.path.split(rdr_filename)
        if not fname.endswith('.h5'):
            LOG.warning("Not an rdr file! Continue")
            return True

        # Check if the file exists:
        if not os.path.exists(rdr_filename):
            LOG.error("File is reported to be dispatched " +
                      "but is not there! File = " +
                      rdr_filename)
            return True

        # Do processing:
        LOG.info("RDR to SDR processing on npp/viirs with CSPP start!" +
                 " Start time = " + str(start_time))
        LOG.info("File = %s" % str(rdr_filename))
        # Fix orbit number in RDR file:
        LOG.info("Fix orbit number in rdr file...")
        try:
            rdr_filename, orbnum = fix_rdrfile(rdr_filename)
        except IOError:
            LOG.exception(
                    'Failed to fix orbit number in RDR file = ' +
                    str(urlobj.path))
        except cspp_runner.orbitno.NoTleFile:
            LOG.exception(
                    'Failed to fix orbit number in RDR file = ' +
                    str(urlobj.path))
            LOG.error('No TLE file...')
        if orbnum:
            self.orbit_number = orbnum
        LOG.info("Orbit number = " + str(self.orbit_number))

        self.glist.append(rdr_filename)

        if len(self.glist) > 4:
            raise RuntimeError("Invalid number of granules to "
                               "process!!!")
        if len(self.glist) == 4:
            LOG.info("4 granules. Skip the first from the list...")
            del self.glist[0]
        if len(self.glist) == 3:
            LOG.info("3 granules. Keep the middle one...")
            keeper = self.glist[1]
        if len(self.glist) == 2:
            LOG.info("2 granules. Keep the first one...")
            keeper = self.glist[0]
        if len(self.glist) == 1:
            # Check start and end time and check if the RDR file
            # contains several granules (a full local swath):
            tdiff = end_time - start_time
            if tdiff.seconds > 4 * 60:
                LOG.info("RDR file contains 3 or more granules. " +
                         "We assume it is a full local swath!")
                keeper = self.glist[0]
                self.fullswath = True
            else:
                LOG.info("Only one granule. This is not enough for CSPP" +
                         " Continue")
                return True

        start_time = get_datetime_from_filename(keeper)
        if self.pass_start_time is None:
            self.pass_start_time = start_time
            LOG.debug("Set the start time of the entire swath: %s", self.pass_start_time.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            LOG.debug("Start time of the entire swath is not changed")

        LOG.info("Before call to spawn_cspp. Argument list = " +
                 str([keeper] + self.glist))
        LOG.info("Start time: %s", start_time.strftime('%Y-%m-%d %H:%M:%S'))
        self.cspp_results.append(
                self.pool.apply_async(
                    spawn_cspp,
                    [keeper] + self.glist,
                    {"viirs_sdr_call": viirs_sdr_call,
                     "viirs_sdr_options": viirs_sdr_options}))
        if self.fullswath:
            LOG.info("Full swath. Break granules loop")
            return False

        return True


def npp_rolling_runner(
        thr_lut_files_age_days,
        url_download_trial_frequency_hours,
        lut_update_stampfile_prefix,
        lut_dir,
        url_jpss_remote_lut_dir,
        mirror_jpss_luts,
        url_jpss_remote_anc_dir,
        anc_update_stampfile_prefix,
        mirror_jpss_ancillary,
        subscribe_topics,
        site,
        mode,
        publish_topic,
        level1_home,
        viirs_sdr_call,
        viirs_sdr_options,
        granule_time_tolerance=10,
        ncpus=1,
        publisher_config=None
        ):
    """The NPP/VIIRS runner. Listens and triggers processing on RDR granules."""

    LOG.info("*** Start the Suomi-NPP/JPSS SDR runner:")
    LOG.info("THR_LUT_FILES_AGE_DAYS = " + str(thr_lut_files_age_days))

    fresh = check_lut_files(
            thr_lut_files_age_days, url_download_trial_frequency_hours,
            lut_update_stampfile_prefix, lut_dir)
    if fresh:
        LOG.info("Files in the LUT dir are fresh...")
        LOG.info("...or download has been attempted recently! " +
                 "No url downloading....")
    else:
        LOG.warning("Files in the LUT dir are non existent or old. " +
                    "Start url fetch...")
        update_lut_files(url_jpss_remote_lut_dir,
                         lut_update_stampfile_prefix, mirror_jpss_luts)

    LOG.info("Dynamic ancillary data will be updated. " +
             "Start url fetch...")
    update_ancillary_files(url_jpss_remote_anc_dir,
                           anc_update_stampfile_prefix, mirror_jpss_ancillary)

    ncpus_available = multiprocessing.cpu_count()
    LOG.info("Number of CPUs available = " + str(ncpus_available))
    LOG.info("Will use %d CPUs when running CSPP instances" % ncpus)
    viirs_proc = ViirsSdrProcessor(ncpus, level1_home)

    if publisher_config is None:
        pubconf = {"name": "viirs_dr_runner", "port": 0}
    else:
        with open(publisher_config, mode="rt", encoding="utf-8") as fp:
            pubconf = yaml.safe_load(fp)

    LOG.debug("Subscribe topics = %s", str(subscribe_topics))
    with posttroll.subscriber.Subscribe('',
                                        subscribe_topics, True) as subscr:
        with Publish(**pubconf) as publisher:
            while True:
                viirs_proc.initialise()
                for msg in subscr.recv(timeout=300):
                    status = viirs_proc.run(
                            msg, viirs_sdr_call, viirs_sdr_options,
                            granule_time_tolerance)
                    LOG.debug("Sent message to run: %s", str(msg))
                    LOG.debug("Status: %s", str(status))
                    if not status:
                        break  # end the loop and reinitialize !

                LOG.debug(
                    "Received message data = %s", str(viirs_proc.message_data))
                proc_start_time = datetime.utcnow()
                tobj = viirs_proc.pass_start_time
                LOG.info("Time used in sub-dir name: " +
                         str(tobj.strftime("%Y-%m-%d %H:%M")))
                subd = create_subdirname(tobj, platform_name=viirs_proc.platform_name,
                                         orbit=viirs_proc.orbit_number)
                LOG.info("Create sub-directory for sdr files: %s" % str(subd))

                LOG.info("Get the results from the multiprocessing pool-run")
                for res in viirs_proc.cspp_results:
                    working_dir, tmp_result_files = res.get()
                    viirs_proc.result_files = tmp_result_files
                    sdr_files = viirs_proc.pack_sdr_files(subd)
                    LOG.info("Cleaning up directory %s" % working_dir)
                    cleanup_cspp_workdir(working_dir)
                    publish_sdr(publisher, sdr_files,
                                viirs_proc.message_data,
                                site, mode, publish_topic,
                                orbit=viirs_proc.orbit_number)

                make_okay_files(viirs_proc.sdr_home, subd)

                LOG.info("Seconds to process SDR: {:.1f}".format(
                    (datetime.utcnow() - proc_start_time).total_seconds()))
                LOG.info("Seconds since granule start: {:.1f}".format(
                    (datetime.utcnow() - tobj).total_seconds()))
                LOG.info("Now that SDR processing has completed, " +
                         "check for new LUT files...")
                fresh = check_lut_files(
                            thr_lut_files_age_days, url_download_trial_frequency_hours,
                            lut_update_stampfile_prefix, lut_dir)
                if fresh:
                    LOG.info("Files in the LUT dir are fresh...")
                    LOG.info("...or download has been attempted recently! " +
                             "No url downloading....")
                else:
                    LOG.warning("Files in the LUT dir are " +
                                "non existent or old. " +
                                "Start url fetch...")
                    update_lut_files(
                        url_jpss_remote_lut_dir,
                        lut_update_stampfile_prefix, mirror_jpss_luts)

                LOG.info("Dynamic ancillary data will be updated. " +
                         "Start url fetch...")
                update_ancillary_files(url_jpss_remote_anc_dir,
                                       anc_update_stampfile_prefix, mirror_jpss_ancillary)
