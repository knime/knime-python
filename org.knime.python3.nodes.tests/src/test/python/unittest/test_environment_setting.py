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

import os
import unittest
from unittest import mock

import knime.extension.env as ke


class TestProxySettings(unittest.TestCase):
    os_env_literal = "os.environ"
    no_proxy_literal = "localhost,127.0.0.1"
    proxy_string_user_pw = "http://user:password@localhost:8080"

    def test_create_proxy_environment_variable_strings(self):
        proxy = ke.ProxySettings(
            protocol_name="http",
            host_name="localhost",
            port_number="8080",
            user_name="user",
            password="password",
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
                user_name="user",
                password="password",
                exclude_hosts=self.no_proxy_literal,
            )
            proxy.set_as_environment_variable()
            self.assertEqual(os.environ.get("http_proxy"), self.proxy_string_user_pw)
            self.assertEqual(os.environ.get("NO_PROXY"), self.no_proxy_literal)

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
                "NO_PROXY": self.no_proxy_literal,
            },
        ):
            proxy = ke.get_proxy_settings("http")
            self.assert_proxy_settings(proxy)

    def assert_proxy_settings(self, proxy):
        self.assertEqual(proxy.protocol_name, "http")
        self.assertEqual(proxy.host_name, "localhost")
        self.assertEqual(proxy.port_number, "8080")
        self.assertEqual(proxy.user_name, "user")
        self.assertEqual(proxy._password, "password")
        self.assertEqual(proxy.exclude_hosts, self.no_proxy_literal)


if __name__ == "__main__":
    unittest.main()
