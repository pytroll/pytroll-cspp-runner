#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013, 2014, 2015, 2016, 2018

# Author(s):

#   Adam.Dybbroe <a000680@c14526.ad.smhi.se>
#   Martin.Raspaud <martin.raspaud@smhi.se>
#   Trygve Aspenes <trygveas@met.no>

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

"""Level-1 processing for VIIRS/ATMS/CRIS Suomi NPP Direct Readout data. Using the CSPP
level-1 processor from the SSEC, Wisconsin, based on the ADL from the NASA DRL.
Listen for pytroll messages from Nimbus (NPP file dispatch) and trigger
processing on direct readout RDR data (granules or full swaths)
"""

import os
import sys
import socket
import logging
import netifaces
from glob import glob
from datetime import datetime, timedelta
try:
    from urllib.parse import urlunsplit, urlparse
except ImportError:
    from urlparse import urlunsplit, urlparse

import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message

from trollsift.parser import compose

import cspp_runner
import cspp_runner.orbitno
from cspp_runner import (get_datetime_from_filename, is_same_granule)
from cspp_runner.post_cspp import (get_sdr_files,
                                   create_subdirname,
                                   pack_sdr_files, make_okay_files,
                                   cleanup_cspp_workdir)
from cspp_runner.pre_cspp import fix_rdrfile

PATH = os.environ.get('PATH', '')

CSPP_SDR_HOME = os.environ.get("CSPP_SDR_HOME", '')
CSPP_RT_SDR_LUTS = os.path.join(CSPP_SDR_HOME, 'anc/cache/incoming_luts')
CSPP_WORKDIR = os.environ.get("CSPP_WORKDIR", '')
APPL_HOME = os.environ.get('NPP_SDRPROC', '')

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "dev"

SDR_SATELLITES = ['Suomi-NPP', 'NOAA-20', 'NOAA-21']

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

_NPP_SDRPROC_LOG_FILE = os.environ.get('NPP_SDRPROC_LOG_FILE', None)

LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
def check_lut_files(thr_days=14):
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
    import stat

    now = datetime.utcnow()

    tdelta = timedelta(
        seconds=float(URL_DOWNLOAD_TRIAL_FREQUENCY_HOURS) * 3600.)
    # Get the time of the last update trial:
    files = glob(LUT_UPDATE_STAMPFILE_RPEFIX + '*')
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
    LOG.info("Directory " + str(LUT_DIR) + "...")
    files = glob(os.path.join(LUT_DIR, '*'))
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


def update_lut_files():
    """
    Function to update the ancillary LUT files

    These files need to be updated at least once every week, in order to
    achieve the best possible SDR processing.

    """
    import os
    from datetime import datetime
    from subprocess import Popen, PIPE, STDOUT

    my_env = os.environ.copy()
    my_env['JPSS_REMOTE_ANC_DIR'] = URL_JPSS_REMOTE_LUT_DIR

    LOG.info("Start downloading....")
    # lftp -c "mirror --verbose --only-newer --parallel=2 $JPSS_REMOTE_ANC_DIR $CSPP_RT_SDR_LUTS"
    # cmdstr = ('lftp -c "mirror --verbose --only-newer --parallel=2 ' +
    #           URL_JPSS_REMOTE_ANC_DIR + ' ' + LUT_DIR + '"')
    cmdstr = OPTIONS['mirror_jpss_luts'] + ' -W {workdir}'.format(workdir=CSPP_WORKDIR)
    LOG.info("Download command: " + cmdstr)

    lftp_proc = Popen(cmdstr, shell=True, env=my_env, stderr=STDOUT, stdout=PIPE)

    while True:
        line = lftp_proc.stdout.readline()
        if not line:
            break
        LOG.info(line.strip())

    lftp_proc.poll()

    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M')
    filename = LUT_UPDATE_STAMPFILE_RPEFIX + '.' + timestamp
    try:
        fpt = open(filename, "w")
        fpt.write(timestamp)
    except OSError:
        LOG.warning('Failed to write LUT-update time-stamp file')
        return
    else:
        fpt.close()

    LOG.info("LUTs downloaded. LUT-update timestamp file = " + filename)

    return


def update_ancillary_files():
    """
    Function to update the dynamic ancillary data.

    These data files encompass Two Line Element (TLE) and Polar Wander (PW)
    files, and should preferably be updated daily. This is done automatically
    in CSPP if the viirs_sdr script is run without the '-l' option. However, to
    slightly speed up the processing and avoid hangups depending on internet
    connectivity this script can omit the automatic download (using the '-l'
    option) and thus the files need to be updated outside the script.

    """
    import os
    from datetime import datetime
    from subprocess import Popen, PIPE, STDOUT

    my_env = os.environ.copy()
    my_env['JPSS_REMOTE_ANC_DIR'] = URL_JPSS_REMOTE_ANC_DIR

    LOG.info("Start downloading dynamic ancillary data " +
             "(TLE and Polar Wander files)....")

    cmdstr = OPTIONS['mirror_jpss_ancillary'] + ' -W {workdir}'.format(workdir=CSPP_WORKDIR)
    LOG.info("Download command: " + cmdstr)

    mirror_proc = Popen(cmdstr, shell=True, env=my_env,
                        stderr=STDOUT, stdout=PIPE)

    while True:
        line = mirror_proc.stdout.readline()
        if not line:
            break
        LOG.info(line.strip())

    mirror_proc.poll()

    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M')
    filename = ANC_UPDATE_STAMPFILE_RPEFIX + '.' + timestamp
    try:
        fpt = open(filename, "w")
        fpt.write(timestamp)
    except OSError:
        LOG.warning('Failed to write ANC-update time-stamp file')
        return
    else:
        fpt.close()

    LOG.info("Ancillary data downloaded")
    LOG.info("Time stamp file = " + filename)

    return


def get_sdr_times(filename):
    """Get the start and end times from the SDR file name
    """
    bname = os.path.basename(filename)
    sll = bname.split('_')
    start_time = datetime.strptime(sll[2] + sll[3][:-1],
                                   "d%Y%m%dt%H%M%S")
    end_time = datetime.strptime(sll[2] + sll[4][:-1],
                                 "d%Y%m%de%H%M%S")
    if end_time < start_time:
        end_time += timedelta(days=1)

    return start_time, end_time


def publish_sdr(publisher, result_files, mda, **kwargs):
    """Publish the messages that SDR files are ready
    """
    if not result_files:
        return

    # Now publish:
    to_send = mda.copy()
    # Delete the RDR uri and uid from the message:
    try:
        del(to_send['uri'])
    except KeyError:
        LOG.warning("Couldn't remove URI from message")
    try:
        del(to_send['uid'])
    except KeyError:
        LOG.warning("Couldn't remove UID from message")

    if 'orbit' in kwargs:
        to_send["orig_orbit_number"] = to_send["orbit_number"]
        to_send["orbit_number"] = kwargs['orbit']

    to_send["dataset"] = []
    for result_file in result_files:
        filename = os.path.basename(result_file)
        to_send[
            'dataset'].append({'uri': urlunsplit(('ssh', socket.gethostname(),
                                                  result_file, '', '')),
                               'uid': filename})
    to_send['format'] = 'SDR'
    to_send['type'] = 'HDF5'
    to_send['data_processing_level'] = '1B'
    to_send['start_time'], to_send['end_time'] = get_sdr_times(filename)

    LOG.debug('Site = %s', SITE)
    if '{' and '}' in PUBLISH_TOPIC:
        try:
            publish_topic = compose(PUBLISH_TOPIC, to_send)
        except:  # noqa
            LOG.debug("Sift topic failed: {} {}".format(PUBLISH_TOPIC, to_send))
            LOG.debug("Be sure to only use available keys.")
            raise
    else:
        publish_topic = '/'.join(('',
                                  PUBLISH_TOPIC,
                                  to_send['format'],
                                  to_send['data_processing_level'],
                                  SITE,
                                  MODE,
                                  'polar',
                                  'direct_readout'))

    LOG.debug('Publish topic = %s', publish_topic)
    msg = Message(publish_topic, "dataset", to_send).encode()
    LOG.debug("sending: " + str(msg))
    publisher.send(msg)


def run_cspp(*rdr_files):
    """Run CSPP on RDR files"""
    from subprocess import Popen, PIPE, STDOUT
    import time
    import tempfile

    sdr_call = OPTIONS[SENSOR + '_sdr_call']
    sdr_options = eval(CONF.get(MODE, SENSOR + '_sdr_options'))
    LOG.info("sdr_options = " + str(sdr_options))
    LOG.info("Path from environment: %s", str(PATH))
    if not isinstance(sdr_options, list):
        LOG.warning("No options will be passed to CSPP")
        sdr_options = []

    try:
        working_dir = tempfile.mkdtemp(dir=CSPP_WORKDIR)
    except OSError:
        working_dir = tempfile.mkdtemp()

    # Run the command:
    cmdlist = [sdr_call]
    cmdlist.extend(sdr_options)
    cmdlist.extend(rdr_files)
    t0_clock = time.clock()
    t0_wall = time.time()
    LOG.info("Popen call arguments: " + str(cmdlist))
    sdr_proc = Popen(cmdlist,
                     cwd=working_dir,
                     stdout=PIPE, stderr=PIPE)
    while True:
        line = sdr_proc.stdout.readline()
        if not line:
            break
        LOG.info(line.decode("utf-8").strip('\n'))

    while True:
        errline = sdr_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline.decode("utf-8").strip('\n'))
    LOG.info("Seconds process time: " + str(time.clock() - t0_clock))
    LOG.info("Seconds wall clock time: " + str(time.time() - t0_wall))

    sdr_proc.poll()
    return working_dir


def spawn_cspp(current_granule, *glist, **kwargs):
    """Spawn a CSPP run on the set of RDR files given"""

    start_time = kwargs.get('start_time')
    platform_name = kwargs.get('platform_name')

    LOG.info("Start CSPP: RDR files = " + str(glist))
    working_dir = run_cspp(*glist)
    LOG.info("CSPP SDR processing finished...")
    # Assume everything has gone well!
    new_result_files = get_sdr_files(working_dir, SENSOR, platform_name=platform_name)
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
    sec_tolerance = int(OPTIONS.get('granule_time_tolerance', 10))
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


class _BaseSdrProcessor(object):

    """
    Base class for SDR processing based on CSPP

    """

    def __init__(self, ncpus):
        from multiprocessing.pool import ThreadPool
        self.pool = ThreadPool(ncpus)
        self.ncpus = ncpus

        self.orbit_number = 1  # Initialised orbit number
        self.platform_name = 'unknown'  # Ex.: Suomi-NPP
        self.sdr_home = OPTIONS['level1_home']
        self.message_data = None
        self.initialise()

    def initialise(self):
        """Initialise the processor"""
        self.fullswath = False
        self.cspp_results = []
        self.glist = []
        self.pass_start_time = None
        self.result_files = []

    @property
    def name(self):
        return self.SENSOR + "_dr_runner"

    def pack_sdr_files(self, subd):
        return pack_sdr_files(self.result_files, self.sdr_home, subd)

    def run(self, msg):
        """Start the SDR processing using CSPP on one rdr granule"""

        if msg:
            LOG.debug("Received message: " + str(msg))

        if self.glist and len(self.glist) > 0:
            LOG.debug("glist: " + str(self.glist))

        if msg is None and self.glist and len(self.glist) > 2:
            # The swath is assumed to be finished now
            LOG.debug("The swath is assumed to be finished now")
            del self.glist[0]
            keeper = self.glist[1]
            LOG.info("Start CSPP: RDR files = " + str(self.glist))
            self.cspp_results.append(self.pool.apply_async(spawn_cspp,
                                                           [keeper] + self.glist))
            LOG.debug("Inside run: Return with a False...")
            return False
        elif msg and ('platform_name' not in msg.data or 'sensor' not in msg.data):
            LOG.debug("No platform_name or sensor in message. Continue...")
            return True
        elif msg and not (msg.data['platform_name'] in SDR_SATELLITES and
                          'viirs' in msg.data['sensor']):
            LOG.info("Not a VIIRS scene. Continue...")
            return True
        elif msg is None:
            return True

        LOG.debug("")
        LOG.debug("\tMessage:")
        LOG.debug(str(msg))
        if type(msg.data['sensor']) in (list,):
            if len(msg.data['sensor']) == 1:
                msg.data['sensor'] = msg.data['sensor'][0]
            else:
                LOG.error("The message sensor element contains more then one sensor name.")
                return False
        urlobj = urlparse(msg.data['uri'])
        LOG.debug("Server = " + str(urlobj.netloc))
        url_ip = socket.gethostbyname(urlobj.netloc)
        if url_ip not in get_local_ips():
            LOG.warning(
                "Server %s not the current one: %s" % (str(urlobj.netloc),
                                                       socket.gethostname()))
            # return True
        LOG.info("Ok... " + str(urlobj.netloc))
        LOG.info("Sat and Instrument: %s %s", str(msg.data['platform_name']),
                 str(msg.data['sensor']))

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

        urlobj = urlparse(msg.data['uri'])
        rdr_filename = urlobj.path
        fname = os.path.basename(rdr_filename)
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
        LOG.info("RDR to SDR processing on npp/%s with CSPP start!" % self.SENSOR +
                 " Start time = " + str(start_time))
        LOG.info("File = %s" % str(rdr_filename))
        # Everything except 0 will set the skip_rdr_orbit_number_fix to True
        skip_rdr_orbit_number_fix = bool(OPTIONS.get('skip_rdr_orbit_number_fix', 0))
        # Fix orbit number in RDR file:
        if not skip_rdr_orbit_number_fix:
            LOG.info("Fix orbit number in rdr file...")
            try:
                rdr_filename, orbnum = fix_rdrfile(rdr_filename)
            except IOError:
                LOG.exception('Failed to fix orbit number in RDR file = %s', str(rdr_filename))
            except cspp_runner.orbitno.NoTleFile:
                LOG.exception('Failed to fix orbit number in RDR file = %s (no TLE file)',
                              str(rdr_filename))

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
        self.cspp_results.append(self.pool.apply_async(spawn_cspp,
                                                       [keeper] + self.glist))
        if self.fullswath:
            LOG.info("Full swath. Break granules loop")
            return False

        return True


class ViirsSdrProcessor(_BaseSdrProcessor):
    """Class for SDR/VIIRS processing based on CSPP
    """
    SENSOR = 'viirs'


class AtmsSdrProcessor(_BaseSdrProcessor):
    """Class for SDR/ATMS processing based on CSPP
    """
    SENSOR = 'atms'


class CrisSdrProcessor(_BaseSdrProcessor):
    """Class for SDR/CRIS processing based on CSPP
    """
    SENSOR = 'cris'


SDR_PROCESSORS = {"viirs": ViirsSdrProcessor,
                  "atms": AtmsSdrProcessor,
                  "cris": CrisSdrProcessor}


def npp_rolling_runner():
    """The NPP (VIIRS, ATMS, CRIS, ...) runner. Listens and triggers processing on RDR granules."""
    from multiprocessing import cpu_count

    LOG.info("*** Start the Suomi-NPP/JPSS SDR runner:")
    if not SKIP_ANC_LUT_UPDATE:
        LOG.info("THR_LUT_FILES_AGE_DAYS = " + str(THR_LUT_FILES_AGE_DAYS))

        fresh = check_lut_files(THR_LUT_FILES_AGE_DAYS)
        if fresh:
            LOG.info("Files in the LUT dir are fresh...")
            LOG.info("...or download has been attempted recently! " +
                     "No url downloading....")
        else:
            LOG.warning("Files in the LUT dir are non existent or old. " +
                        "Start url fetch...")
            update_lut_files()

        LOG.info("Dynamic ancillary data will be updated. " +
                 "Start url fetch...")
        update_ancillary_files()

    ncpus_available = cpu_count()
    LOG.info("Number of CPUs available = " + str(ncpus_available))
    ncpus = int(OPTIONS.get('ncpus', 1))
    LOG.info("Will use %d CPUs when running CSPP instances" % ncpus)

    sdr_proc = SDR_PROCESSORS[SENSOR](ncpus)

    LOG.debug("Subscribe topics = %s", str(SUBSCRIBE_TOPICS))
    services = OPTIONS.get('services', '').split(',')
    LOG.debug("Subscribing to services: {}".format(services))
    with posttroll.subscriber.Subscribe(services,
                                        SUBSCRIBE_TOPICS, True) as subscr:
        with Publish(sdr_proc.name, 0) as publisher:
            while True:
                sdr_proc.initialise()
                for msg in subscr.recv(timeout=SUBSCRIBE_RECV_TIMEOUT):
                    status = sdr_proc.run(msg)
                    if not status:
                        break  # end the loop and reinitialize !

                LOG.debug(
                    "Received message data = %s", str(sdr_proc.message_data))
                tobj = sdr_proc.pass_start_time
                LOG.info("Time used in sub-dir name: " +
                         str(tobj.strftime("%Y-%m-%d %H:%M")))
                if 'subdir' in OPTIONS:
                    subd = create_subdirname(tobj, platform_name=sdr_proc.platform_name,
                                             orbit=sdr_proc.orbit_number, subdir=OPTIONS.get('subdir'),
                                             sensor=msg.data['sensor'])
                else:
                    #This is the default and original
                    subd = create_subdirname(tobj, platform_name=sdr_proc.platform_name,
                                             orbit=sdr_proc.orbit_number)
                LOG.info("Create sub-directory for sdr files: %s" % str(subd))

                LOG.info("Get the results from the multiptocessing pool-run")
                for res in sdr_proc.cspp_results:
                    working_dir, tmp_result_files = res.get()
                    sdr_proc.result_files = tmp_result_files
                    sdr_files = sdr_proc.pack_sdr_files(subd)
                    LOG.info("Cleaning up directory %s" % working_dir)
                    cleanup_cspp_workdir(working_dir)
                    publish_sdr(publisher, sdr_files,
                                sdr_proc.message_data,
                                orbit=sdr_proc.orbit_number)

                if not SKIP_ANC_LUT_UPDATE:
                    LOG.info("Now that SDR processing has completed, " +
                             "check for new LUT files...")
                    fresh = check_lut_files(THR_LUT_FILES_AGE_DAYS)
                    if fresh:
                        LOG.info("Files in the LUT dir are fresh...")
                        LOG.info("...or download has been attempted recently! " +
                                 "No url downloading....")
                    else:
                        LOG.warning("Files in the LUT dir are " +
                                    "non existent or old. " +
                                    "Start url fetch...")
                        update_lut_files()

                    LOG.info("Dynamic ancillary data will be updated. " +
                             "Start url fetch...")
                    update_ancillary_files()

    return


# ---------------------------------------------------------------------------
if __name__ == "__main__":

    from logging import handlers
    import argparse
    try:
        import configparser
    except ImportError:
        import ConfigParser as configparser

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file",
                        required=True,
                        dest="config_file",
                        type=str,
                        default=None,
                        help="The file containing configuration parameters.")

    parser.add_argument("-C", "--section",
                        required=True,
                        dest="section",
                        type=str,
                        default=None,
                        help="The section in the config file.")

    parser.add_argument("-l", "--log-file", dest="log",
                        type=str,
                        default=None,
                        help="The file to log to (stdout per default).")

    parser.add_argument("-s", "--sensor",
                        required=True,
                        dest="sensor",
                        type=str,
                        default=None,
                        help="Sensor type to process (viirs, atms, cris, ...).")

    args = parser.parse_args()
    SENSOR = args.sensor
    MODE = args.section

    CONF = configparser.ConfigParser()

    print("Read config from %s (section %s)" % (args.config_file, args.section))
    CONF.read(args.config_file)

    OPTIONS = {}
    for option, value in CONF.items(MODE, raw=True):
        OPTIONS[option] = value
    PUBLISH_TOPIC = OPTIONS.get('publish_topic')
    SUBSCRIBE_TOPICS = OPTIONS.get('subscribe_topics').split(',')
    SUBSCRIBE_RECV_TIMEOUT = float(OPTIONS.get('subscribe_recv_timeout', 300))

    for item in SUBSCRIBE_TOPICS:
        if len(item) == 0:
            SUBSCRIBE_TOPICS.remove(item)

    SITE = OPTIONS.get('site')

    if args.log is not None:
        ndays = int(OPTIONS.get("log_rotation_days", 1))
        ncount = int(OPTIONS.get("log_rotation_backup", 7))
        handler = handlers.TimedRotatingFileHandler(args.log,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('cspp_dr_runner')

    if SENSOR not in SDR_PROCESSORS:
        LOG.error("Unknown sensor '%s' (not in %s)", SENSOR, str(SDR_PROCESSORS.keys()))
        sys.exit(2)

    SKIP_ANC_LUT_UPDATE = False
    try:
        THR_LUT_FILES_AGE_DAYS = OPTIONS.get('threshold_lut_files_age_days', 14)
        URL_JPSS_REMOTE_LUT_DIR = OPTIONS['url_jpss_remote_lut_dir']
        URL_JPSS_REMOTE_ANC_DIR = OPTIONS['url_jpss_remote_anc_dir']
        LUT_DIR = OPTIONS.get('lut_dir', CSPP_RT_SDR_LUTS)
        LUT_UPDATE_STAMPFILE_RPEFIX = OPTIONS['lut_update_stampfile_prefix']
        ANC_UPDATE_STAMPFILE_RPEFIX = OPTIONS['anc_update_stampfile_prefix']
        URL_DOWNLOAD_TRIAL_FREQUENCY_HOURS = OPTIONS[
            'url_download_trial_frequency_hours']
    except:  # noqa
        LOG.info("One or more of the lut or anc config variables are not given. Will not update any of those")
        LOG.info("Be sure to run you sdr script without the -l flag to keep your luts and ancillary data updated")
        SKIP_ANC_LUT_UPDATE = True

    npp_rolling_runner()
