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

"""
"""
import socket
from trollsift import Parser, globify
import os
import pathlib
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

                #platform_name = msg.data.get('platform_name')

                wrkdir = run_atms_from_message(msg, self._atms_sdr_call, self._atms_sdr_options)

                logger.warning("Do nothing...")

                # filename = get_filename_from_uri(msg.data.get('uri'))
                # if not os.path.exists(filename):
                #     logger.warning("File does not exist!")
                #     continue

                # file_ok = check_file_type_okay(msg.data.get('type'))

                # for output_msg in output_messages:
                #     if output_msg:
                #         logger.debug("Sending message: %s", str(output_msg))
                #         self.publisher.send(str(output_msg))


def run_atms_from_message(posttroll_msg, sdr_call, sdr_options):
    """Trigger ATMS processing on ATMS scene, from Posttroll message."""

    platform_name = posttroll_msg.data.get('platform_name')
    sensor = posttroll_msg.data.get('sensor')

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

    my_env = os.environ.copy()

    # Run the command:
    cmdlist = [sdr_call]
    cmdlist.extend(sdr_options)
    cmdlist.extend(atms_rdr_files)

    t0_clock = time.process_time()
    t0_wall = time.time()
    logger.info("Popen call arguments: " + str(cmdlist))

    sdr_proc = subprocess.Popen(cmdlist, cwd=cspp_workdir,
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

    return cspp_workdir


def get_filelist_from_collection(atms_collection):
    """From a posttroll message extract the ATMS files from collection."""
    filelist = []
    for obj in atms_collection:
        urlobj = urlparse(obj['uri'])
        filelist.append(urlobj.path)

    return filelist
