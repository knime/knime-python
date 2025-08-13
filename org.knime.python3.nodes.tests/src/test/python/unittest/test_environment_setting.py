# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

import logging
import os
import tempfile
from urllib.parse import quote_plus
import unittest
from unittest import mock

import knime.extension.env as ke  # old import to test forwarding
from knime.api import env

logger = logging.getLogger(__name__)


class TestProxySettings(unittest.TestCase):
    os_env_literal = "os.environ"
    no_proxy_literal = "localhost,127.0.0.1"
    proxy_string_user_pw = (
        f"""http://{quote_plus("us@r!")}:{quote_plus("p@ss:word")}@localhost:8080"""
    )

    def test_create_proxy_environment_variable_strings(self):
        proxy = ke.ProxySettings(
            protocol_name="http",
            host_name="localhost",
            port_number="8080",
            user_name="us@r!",
            password="p@ss:word",
        )
        variable, value = proxy.create_proxy_environment_key_value_pair()
        self.assertEqual(variable, "http_proxy")
        self.assertEqual(value, self.proxy_string_user_pw)

    def test_set_as_environment_variable(self):
        with mock.patch(self.os_env_literal, {}):
            proxy = ke.ProxySettings(
                protocol_name="http",
                host_name="localhost",
                port_number="8080",
                user_name="us@r!",
                password="p@ss:word",
                exclude_hosts=self.no_proxy_literal,
            )
            proxy.set_as_environment_variable()
            self.assertEqual(os.environ.get("http_proxy"), self.proxy_string_user_pw)
            self.assertEqual(os.environ.get("no_proxy"), self.no_proxy_literal)

    def test_from_string(self):
        proxy = ke.ProxySettings.from_string(
            proxy_string=self.proxy_string_user_pw, exclude_hosts=self.no_proxy_literal
        )
        self.assert_proxy_settings(proxy)

    def test_get_proxy_settings(self):
        with mock.patch(
            self.os_env_literal,
            {
                "http_proxy": self.proxy_string_user_pw,
                "no_proxy": self.no_proxy_literal,
            },
        ):
            proxy = ke.get_proxy_settings("http")
            self.assert_proxy_settings(proxy)

    def assert_proxy_settings(self, proxy):
        self.assertEqual(proxy.protocol_name, "http")
        self.assertEqual(proxy.host_name, "localhost")
        self.assertEqual(proxy.port_number, "8080")
        self.assertEqual(proxy.user_name, "us@r!")
        self.assertEqual(proxy.password, "p@ss:word")
        self.assertEqual(proxy.exclude_hosts, self.no_proxy_literal)

    def test_java_to_python_no_proxy_translation(self):
        # fmt: off
        for raw, expected in {
            "*.example.com": ".example.com",  # star-dot prefix -> leading dot
            "*example.com" : ".example.com",  # star prefix     -> leading dot
            "*.*.knime.com": ".knime.com",    # multiple stars  -> collapse & single dot
            "example.com"  : "example.com",   # no wildcard     -> unchanged
            "*.*.*"        : "*",             # only stars/dots -> star
            "*"            : "*",             # single star     -> preserved
        }.items():
        # fmt: on
            self.assertEqual(
                ke.ProxySettings._translate_excluded_host(raw),
                expected,
            )

        java_no_proxy = "localhost|*.example.com|*example.org|*.*.knime.com|*"
        python_no_proxy = "localhost,.example.com,.example.org,.knime.com,*"

        with mock.patch(self.os_env_literal, {}):
            proxy = ke.ProxySettings(
                protocol_name="http",
                host_name="localhost",
                exclude_hosts=java_no_proxy,
            )
            proxy.set_as_environment_variable()
            self.assertEqual(
                os.environ.get("no_proxy"),
                python_no_proxy
            )


class TestTmpDirectorySettings(unittest.TestCase):
    class JavaCallback(object):
        """
        Class to mimic Java callback after temporary path changes in Preferences.
        """

        def __init__(self, value):
            self.value = value

        def get_global_tmp_dir_path(self):
            return self.value

    @classmethod
    def setUpClass(cls):
        # Save path before test
        cls.old_path = os.environ.get("TEMP", tempfile.gettempdir())
        cls.new_path = os.path.join(cls.old_path, "TestTmpDirectorySettings")
        logger.info(
            f"Start TestTmpDirectorySettings  old_path={cls.old_path}, new_path={cls.new_path}"
        )
        os.mkdir(cls.new_path)

    @classmethod
    def tearDownClass(cls):
        # Restore an original path from the build pipeline
        java_callback = cls.JavaCallback(cls.old_path)
        env._set_tmp_directory(java_callback)
        logger.info(
            f"Teardown tests and old_path={cls.old_path}, new_path={cls.new_path}"
        )
        # Cleanup
        os.rmdir(cls.new_path)

    def test_set_as_environment_variable(self):
        expected_text = "Lorem ipsum"

        java_callback = self.JavaCallback(self.new_path)
        env._set_tmp_directory(java_callback)

        # Check environment variables
        assert self.new_path == os.environ["TMPDIR"]
        assert self.new_path == os.environ["TMP"]
        assert self.new_path == os.environ["TEMP"]

        # Check temp path set
        current_temp_dir = tempfile.gettempdir()
        assert current_temp_dir == self.new_path

        # Create a temp file
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            file_name = f.name
            f.write(expected_text)

        # Check that file was created in a temp path
        assert self.new_path in file_name

        # Additionally, check content
        with open(file_name, "r") as f:
            actual_text = f.read()

        # Cleanup tmp file
        os.remove(file_name)

        assert expected_text == actual_text


if __name__ == "__main__":
    unittest.main()
