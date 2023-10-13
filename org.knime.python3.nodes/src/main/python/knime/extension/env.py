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

"""
Provides access to environment variables and other information about the KNIME Python integration.

@author Jonas Klotz, KNIME GmbH, Berlin, Germany
"""
import os
from dataclasses import dataclass
from typing import Optional, Tuple
import logging

supported_proxy_protocols = ["http", "https"]


@dataclass
class ProxySettings:
    """Proxy settings for a KNIME node"""

    protocol_name: Optional[str]
    host_name: Optional[str]
    port_number: Optional[str]
    exclude_hosts: Optional[str]
    user_name: Optional[str]
    password: Optional[str]

    def __init__(
        self,
        protocol_name: Optional[str] = None,
        host_name: Optional[str] = None,
        port_number: Optional[str] = None,
        exclude_hosts: Optional[str] = None,
        user_name: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.protocol_name = protocol_name.lower()

        self.host_name = host_name
        self.port_number = port_number
        self.exclude_hosts = exclude_hosts
        self.user_name = user_name
        self._password = password

        self._has_credentials = user_name and password

    def __str__(self):
        """Create the proxy environment variable string"""

        if self.protocol_name not in supported_proxy_protocols:
            logging.warning(
                f"The KNIME proxy settings currently only support the following protocols: "
                f"{supported_proxy_protocols}, but the protocol {self.protocol_name} was provided."
                f"The KNIME proxy settings will be ignored for python nodes."
            )
            return ""

        proxy_env_string = f"{self.protocol_name}://"

        if self._has_credentials:
            proxy_env_string += f"{self.user_name}:{self._password}@"

        proxy_env_string += f"{self.host_name}"

        if self.port_number:
            proxy_env_string += f":{self.port_number}"

        return proxy_env_string

    def create_proxy_environment_key_value_pair(self) -> Tuple[str, str]:
        """Create the proxy environment variable strings

        Returns:
            Tuple[str, str]: The proxy environment variable name and value
        """
        if not (self.protocol_name and self.host_name):
            return "", ""
        return f"{self.protocol_name}_proxy", str(self)

    def set_as_environment_variable(self):
        """Set the proxy settings as environment variables"""
        (
            proxy_env_variable,
            proxy_env_string,
        ) = self.create_proxy_environment_key_value_pair()
        if proxy_env_variable and proxy_env_string:
            os.environ[proxy_env_variable] = proxy_env_string
        if self.exclude_hosts:
            os.environ["NO_PROXY"] = self.exclude_hosts

    @classmethod
    def from_string(cls, proxy_string, exclude_hosts: Optional[str] = None):
        """Parse the proxy settings from a string

        Args:
            proxy_string: The string is in the format of:
                        protocol://user:password@host:port or protocol://host:port
                        e.g. http://user:password@localhost:8080 or http://localhost:8080
            exclude_hosts: The hosts that should be excluded from the proxy, e.g. localhost,
                            separated by a comma

        Returns:
            ProxySettings: The proxy settings object
        """

        # Parse the protocol
        protocol_name, proxy_string = proxy_string.split("://", 1)
        protocol_name = protocol_name.lower()
        if protocol_name not in supported_proxy_protocols:
            raise ValueError(
                f"Invalid or unsupported protocol: {protocol_name}, to see all supported protocols, "
                f"call ProxySettings.supported_proxy_protocols()"
            )

        # Parse the user and password
        user_name, password = None, None
        if "@" in proxy_string:
            # The user and password is provided
            user_password, proxy_string = proxy_string.split("@", 1)
            user_name, password = user_password.split(":", 1)

        # Parse the host and port
        host_name, port_number = None, None
        if ":" in proxy_string:
            host_name, port_number = proxy_string.split(":", 1)
        else:  # if no port is provided
            host_name = proxy_string

        return cls(
            protocol_name=protocol_name,
            host_name=host_name,
            port_number=port_number,
            exclude_hosts=exclude_hosts,
            user_name=user_name,
            password=password,
        )

    @staticmethod
    def supported_proxy_protocols() -> str:
        return f"""
        The KNIME proxy settings for python currently only support the following protocols:
        {supported_proxy_protocols}
        """


def get_proxy_settings(protocol_name: Optional[str] = None) -> Optional[ProxySettings]:
    """Get the proxy settings from the environment variables

    Get the proxy settings as configured either in KNIME’s preferences or via environment variables.
    Even if the proxy settings were configured in KNIME’s preferences only, they are already set as
    environment variables for this Python process, so they are in effect for everything you do.

    Args:
        protocol_name(str): The protocol name, e.g. 'http' or 'https'. To see all supported protocols,
                            call ProxySettings.supported_proxy_protocols().

    Returns:
        ProxySettings: The proxy settings object
    """
    if not protocol_name:
        for p_name in supported_proxy_protocols:
            proxy_settings = get_proxy_settings(p_name)
            if proxy_settings:
                return proxy_settings
        raise ValueError(
            f"Could not find any proxy settings for the supported protocols: {supported_proxy_protocols}"
        )

    protocol_name = protocol_name.lower()
    if protocol_name not in supported_proxy_protocols:
        raise ValueError(
            f"Unsupported protocol: {protocol_name}. Supported protocols: {supported_proxy_protocols}"
        )

    proxy_env_variable = f"{protocol_name}_proxy"
    proxy_env_string = os.environ.get(proxy_env_variable)
    if not proxy_env_string:
        return None

    exclude_hosts = os.environ.get("NO_PROXY", None)
    return ProxySettings.from_string(proxy_env_string, exclude_hosts)


def _set_proxy_settings(java_callback):
    """Set proxy settings to environment variable from Java callback

    Args:
        java_callback: The Java callback object. Must have the following methods:
                        - get_proxy_server_strings() -> JavaArray[str]
    """
    proxy_settings = ProxySettings(*list(java_callback.get_proxy_server_strings()))
    proxy_settings.set_as_environment_variable()
