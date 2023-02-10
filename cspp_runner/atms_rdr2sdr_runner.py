#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Pytroll developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

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

"""Run the ATMS RDR to SDR processing withy CSPP on incoming DR data."""


import socket
from trollsift import Parser, globify
import os
import shutil

import pathlib
import tempfile
from glob import glob
from datetime import datetime

import subprocess
from urllib.parse import urlparse
import logging
import time

import signal
from queue import Empty
from threading import Thread
from posttroll.listener import ListenerContainer
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher
from cspp_runner.config import read_config
from cspp_runner.runner import check_environment
from cspp_runner.constants import (PLATFORM_SHORTNAMES,
                                   PLATFORM_LONGNAMES)


logger = logging.getLogger(__name__)


class AtmsSdrRunner(Thread):
    """The ATMS CSPP runner to process RDR to SDR files."""

    def __init__(self, configfile):
        """Initialize the runner object."""
        super().__init__()

        self.configfile = configfile
        self.options = {}
        config = read_config(self.configfile)
        self.options = config

        self.host = socket.gethostname()

        self.sdr_file_patterns = self.options['sdr_file_patterns']
        self._sdr_home = self.options['level1_home']

        self.input_topics = self.options['subscribe_topics']
        self.output_topics = self.options['publish_topics']

        self._atms_sdr_call = self.options['atms_sdr_call']
        self._atms_sdr_options = self.options['atms_sdr_options']

        self.listener = None
        self.publisher = None
        self.loop = False
        self._setup_and_start_communication()

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        logger.debug("Starting up... Input topics:")
        for top in self.input_topics:
            logger.debug("{topic}".format(topic=str(top)))

        self.listener = ListenerContainer(topics=self.input_topics)
        self.publisher = NoisyPublisher("atms-rdr2sdr-runner")
        self.publisher.start()
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def signal_shutdown(self, *args, **kwargs):
        """Shutdown the ATMS rdr2sdr runner."""
        self.close()

    def run(self):
        """Run the CSPP ATMS RDR2SDR processing."""
        while self.loop:
            try:
                msg = self.listener.output_queue.get(timeout=1)
                logger.debug("Message: %s", str(msg.data))
            except Empty:
                continue
            else:
                if msg.type not in ['file', 'collection', 'dataset']:
                    logger.debug("Message type not supported: %s", str(msg.type))
                    continue

                wrkdir = run_atms_from_message(msg, self._atms_sdr_call, self._atms_sdr_options)

                logger.info("ATMS RDR to SDR processing finished")
                logger.debug("Start packing the files and publish")

                sdr_filepaths = get_filepaths(wrkdir, msg.data, self.sdr_file_patterns)
                logger.debug("Files: %s", str(sdr_filepaths))

                dest_sdr_files = move_files_to_destination(sdr_filepaths,
                                                           self.sdr_file_patterns, self._sdr_home)
                logger.debug("Files after having been moved: %s", str(dest_sdr_files))

                orbit_number = _fix_orbit_number(dest_sdr_files, self.sdr_file_patterns)
                output_messages = self._get_output_messages(dest_sdr_files, msg, orbit_number)

                for output_msg in output_messages:
                    if output_msg:
                        logger.debug("Sending message: %s", str(output_msg))
                        self.publisher.send(str(output_msg))

    def _get_output_messages(self, sdr_files, input_msg, orbit_number):
        """Generate output messages from SDR files and input message, and return."""
        out_messages = []
        for topic in self.output_topics:
            to_send = prepare_posttroll_message(input_msg)
            dataset = []
            for filepath in sdr_files:
                sdrfile = {}
                sdrfile['uri'] = 'ssh://{host}/{path}'.format(host=self.host, path=filepath)
                sdrfile['uid'] = os.path.basename(filepath)
                dataset.append(sdrfile)

            to_send['type'] = 'HDF5'
            to_send['format'] = 'SDR'
            to_send['data_processing_level'] = '1B'
            to_send['dataset'] = dataset
            to_send['orig_orbit_number'] = to_send.get('orbit_number')
            to_send['orbit_number'] = orbit_number

            pubmsg = Message(topic, 'dataset', to_send)
            out_messages.append(pubmsg)

        return out_messages

    def close(self):
        """Shutdown the ATMS SDR processing."""
        logger.info('Terminating ATMS RDR to SDR processing.')
        self.loop = False
        try:
            self.listener.stop()
        except Exception:
            logger.exception("Couldn't stop listener.")
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                logger.exception("Couldn't stop publisher.")


def _fix_orbit_number(sdr_files, sdr_file_patterns):
    """Get the orbit number from the SDR files produced with CSPP."""
    s_pattern = get_tb_files_pattern(sdr_file_patterns)

    p__ = Parser(s_pattern)
    orbit_numbers = []
    for filename in sdr_files:
        bname = os.path.basename(str(filename))
        logger.debug("SDR filename: %s", str(bname))
        try:
            result = p__.parse(bname)
        except ValueError:
            continue

        orbit = result.get('orbit', 0)
        logger.debug("Orbit number = %s", orbit)
        orbit_numbers.append(orbit)

    # Test if there are more orbit numbers and at least log an info.
    # FIXME!
    return orbit_numbers[0]


def prepare_posttroll_message(input_msg):
    """Create the basic posttroll-message fields and return."""
    to_send = input_msg.data.copy()
    to_send.pop('dataset', None)
    to_send.pop('collection', None)
    to_send.pop('uri', None)
    to_send.pop('uid', None)
    to_send.pop('format', None)
    to_send.pop('type', None)

    return to_send


def move_files_to_destination(sdr_filepaths, sdr_file_patterns, sdr_home):
    """Move the SDR files from tmp-directory to a final destination."""
    dirpath = create_subdir_from_filepaths(sdr_filepaths, sdr_file_patterns, sdr_home)
    for filename in sdr_filepaths:
        shutil.move(filename, dirpath)

    return glob(str(dirpath / "*"))


def get_tb_files_pattern(sdr_file_patterns):
    """Get the file name pattern for the TB SDR files (SATMS)."""
    for pattern in sdr_file_patterns:
        if pattern.startswith('S'):
            return pattern


def create_subdir_from_filepaths(sdr_filepaths, sdr_file_patterns, sdr_home):
    """From the list of SDR files create a sub-directory where files should be moved."""
    s_pattern = get_tb_files_pattern(sdr_file_patterns)

    start_time = datetime.now()
    p__ = Parser(s_pattern)

    orbit = 0
    platform = 'unknown'
    for filename in sdr_filepaths:
        bname = os.path.basename(str(filename))
        logger.debug("SDR filename: %s", str(bname))
        try:
            result = p__.parse(bname)
        except ValueError:
            continue

        stime = result.get('start_time')
        if stime and stime < start_time:
            start_time = stime
            orbit = result.get('orbit', 0)
            platform = PLATFORM_LONGNAMES.get(result['platform_shortname'])

    subdirname = "{platform}_{dtime:%Y%m%d_%H%M}_{orbit:05d}".format(platform=platform.lower().replace('-', ''),
                                                                     dtime=start_time, orbit=orbit)
    if isinstance(sdr_home, str):
        sdr_home = pathlib.Path(sdr_home)

    dirpath = sdr_home / subdirname
    dirpath.mkdir()
    return dirpath


def get_filepaths(directory, msg_data, file_patterns):
    """Identify the ATMS files output from CSPP and return filepaths."""
    files = []
    for pattern in file_patterns:
        # p__ = Parser(pattern)
        # mda = {'orbit': msg_data['orbit_number'],
        #        'platform_shortname': PLATFORM_SHORTNAMES.get(msg_data['platform_name'])}
        mda = {'platform_shortname': PLATFORM_SHORTNAMES.get(msg_data['platform_name'])}

        # 'start_time': msg_data['start_time']} Here check for times, if
        # start/end times in the file names are sufficiently close to the
        # actual ones from the messages

        glbstr = globify(pattern, mda)
        logger.debug("Glob-string = %s", str(glbstr))
        flist = glob(os.path.join(directory, glbstr))
        files = files + flist

    logger.debug("Files: %s", str(files))
    return files


def run_atms_from_message(posttroll_msg, sdr_call, sdr_options):
    """Trigger ATMS processing on ATMS scene, from Posttroll message."""
    # platform_name = posttroll_msg.data.get('platform_name')
    # sensor = posttroll_msg.data.get('sensor')
    collection = posttroll_msg.data.get('collection')
    if collection:
        atms_rdr_files = get_filelist_from_collection(collection)
    else:
        logger.warning("ATMS processing so far only supports running on collection of files.")
        return

    # Process ATMS files in a sub process:
    check_environment("CSPP_WORKDIR")
    cspp_workdir = os.environ.get("CSPP_WORKDIR", '')
    pathlib.Path(cspp_workdir).mkdir(parents=True, exist_ok=True)

    try:
        working_dir = tempfile.mkdtemp(dir=cspp_workdir)
    except OSError:
        working_dir = tempfile.mkdtemp()

    os.environ["CSPP_WORKDIR"] = working_dir

    my_env = os.environ.copy()

    # Run the command:
    cmdlist = [sdr_call]
    cmdlist.extend(sdr_options)
    cmdlist.extend(atms_rdr_files)

    t0_clock = time.process_time()
    t0_wall = time.time()
    logger.info("Popen call arguments: " + str(cmdlist))

    sdr_proc = subprocess.Popen(cmdlist, cwd=working_dir,
                                env=my_env,
                                stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE)
    while True:
        line = sdr_proc.stdout.readline()
        if not line:
            break
        logger.info(line.decode("utf-8").strip('\n'))

    while True:
        errline = sdr_proc.stderr.readline()
        if not errline:
            break
        logger.info(errline.decode("utf-8").strip('\n'))

    logger.info("Seconds process time: " + (str(time.process_time() - t0_clock)))
    logger.info("Seconds wall clock time: " + (str(time.time() - t0_wall)))

    sdr_proc.poll()

    return working_dir


def get_filelist_from_collection(atms_collection):
    """From a posttroll message extract the ATMS files from collection."""
    filelist = []
    for obj in atms_collection:
        urlobj = urlparse(obj['uri'])
        filelist.append(urlobj.path)

    return filelist
