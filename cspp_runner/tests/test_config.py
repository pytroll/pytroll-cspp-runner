#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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


"""Test getting the yaml configurations from file."""

from cspp_runner.config import read_config


def test_get_yaml_configuration(fake_yamlconfig_file):
    """Test read and get the yaml configuration from file."""
    config = read_config(fake_yamlconfig_file)

    assert len(config['subscribe_topics']) == 1
    assert config['subscribe_topics'][0] == '/file/atms/rdr'
    assert len(config['publish_topics']) == 1
    assert config['publish_topics'][0] == '/file/atms/sdr'

    assert config['level1_home'] == '/path/to/where/the/atms/sdr/files/will/be/stored'
    assert config['working_dir'] == '/san1/cspp/work'
    assert config['atms_sdr_call'] == 'atms_sdr.sh'
    assert config['atms_sdr_options'] == ['-a', '-d']
