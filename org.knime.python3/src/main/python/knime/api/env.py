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
import platform
import re
import sys
import tempfile
from urllib.parse import urlsplit, quote_plus, unquote_plus

try:
    from dataclasses import dataclass
except ImportError:
    from dataclasses_py36backport import dataclass
from typing import Optional, Tuple
import logging

supported_proxy_protocols = ["http", "https"]
no_proxy_key = "no_proxy"


@dataclass
class ProxySettings:
    """
    Proxy settings for a KNIME node

    The proxy settings are used to set the proxy environment variables for the KNIME Python integration.

    Attributes
    ----------
        protocol_name : str or None
            The lowercase protocol name.
        host_name : str or None
            The host name.
        port_number : str or None
            The port number.
        exclude_hosts : str or None
            List of hosts to exclude.
        user_name : str or None
            The username.
        password : str or None
            The password.
        has_credentials : bool
            True if both username and password are provided, False otherwise.

    Parameters
    ----------
        protocol_name : str, optional
            The name of the protocol. Default is None.
        host_name : str, optional
            The name of the host. Default is None.
        port_number : str, optional
            The port number. Default is None.
        exclude_hosts : str, optional
            List of hosts to exclude. Default is None.
        user_name : str, optional
            The username. Default is None.
        password : str, optional
            The password. Default is None.

    Methods
    -------
        create_proxy_environment_key_value_pair()
            Create the proxy environment variable strings.
        set_as_environment_variable()
            Set the proxy settings as environment variables.
        from_string(proxy_string, exclude_hosts=None)
            Parse the proxy settings from a string.
        supported_proxy_protocols()
            Return the supported proxy protocols for KNIME proxy settings in Python.

    """

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
        """
        Initialize a connection object with optional parameters.
        """
        self.protocol_name = protocol_name.lower() if protocol_name else None

        self.host_name = host_name
        self.port_number = port_number
        self.exclude_hosts = exclude_hosts
        self.user_name = user_name
        self.password = password

    def has_credentials(self) -> bool:
        return self.user_name is not None and self.password is not None

    def __str__(self):
        """
        Create the proxy environment variable string.
        """

        if not self.protocol_name:
            return ""

        if self.protocol_name not in supported_proxy_protocols:
            logging.warning(
                f"The KNIME proxy settings currently only support the following protocols: "
                f"{supported_proxy_protocols}, but the protocol {self.protocol_name} was provided."
                f"The KNIME proxy settings will be ignored for python nodes."
            )
            return ""

        # KNIME always uses http to talk to the proxy, no matter which protocol.
        # The protocol (which depends on the target URL) is only used to determine which proxy to use.
        proxy_env_string = "http://"

        if self.has_credentials():
            proxy_env_string += (
                f"{quote_plus(self.user_name)}:{quote_plus(self.password)}@"
            )

        proxy_env_string += f"{self.host_name}"

        if self.port_number:
            proxy_env_string += f":{self.port_number}"

        return proxy_env_string

    def create_proxy_environment_key_value_pair(self) -> Tuple[str, str]:
        """
        Create the proxy environment variable strings.

        Returns
        -------
            Tuple[str, str]: The proxy environment variable name and value
        """
        if not (self.protocol_name and self.host_name):
            return "", ""
        return f"{self.protocol_name}_proxy", str(self)

    def set_as_environment_variable(self):
        """
        Set the proxy settings as environment variables.
        """
        if self.protocol_name not in supported_proxy_protocols:
            logging.warning(
                f"The KNIME proxy settings currently only support the following protocols: "
                f"{supported_proxy_protocols}, but the protocol {self.protocol_name} was provided."
                f"The KNIME proxy settings will be ignored for python nodes."
            )
            return
        (
            proxy_env_variable,
            proxy_env_string,
        ) = self.create_proxy_environment_key_value_pair()
        if proxy_env_variable and proxy_env_string:
            os.environ[proxy_env_variable] = proxy_env_string
        if self.exclude_hosts:
            excluded_hosts_list = []
            for host in self.exclude_hosts.split("|"):
                excluded_hosts_list.append(ProxySettings._translate_excluded_host(host))

            # Replace '|' with ',' in NO_PROXY because Java uses '|' as a separator,
            # but python-httpx expects ',' for the NO_PROXY environment variable.
            os.environ[no_proxy_key] = ",".join(excluded_hosts_list)

    @staticmethod
    def _translate_excluded_host(host: str) -> str:
        """
        Translation from a Java-accepted excluded host to a Python-accepted excluded host
        for proxies. The translation itself is not as trivial, e.g. since Python treats wildcards
        as plain literals, but - on the other hand - employs additional suffix matching.

        See "https://about.gitlab.com/blog/we-need-to-talk-no-proxy/#the-lowest-common-denominator".

        Returns
        -------
            str: The excluded host that can be set (or ","-joined) as "no_proxy" environment variable
        """
        if host is None:
            return ""

        host = host.strip()
        if host == "*":
            # Proxy is disabled for all hosts.
            return "*"
        # Suffix matching requires a "leading-dot" syntax.
        suffix = re.match(r"^\*[^.]", host)
        # If removing wildcards causes an invalid URL (e.g. "*.*.knime.com" -> "..knime.com"),
        # replace multiple dots (2 or more) with a single one.
        host = re.sub(r"\.{2,}", ".", host.replace("*", ""))
        # If we get a single dot, proxy is disabled for all hosts (e.g. "*.*" -> ".").
        if host == ".":
            return "*"

        return f".{host}" if suffix else host

    @classmethod
    def from_string(cls, proxy_string, exclude_hosts: Optional[str] = None):
        """
        Parse the proxy settings from a string

        Parameters
        ----------
        proxy_string : str
            The string is in the format of:
            protocol://user:password@host:port or protocol://host:port
            e.g. http://user:password@localhost:8080 or http://localhost:8080

        exclude_hosts : str
            The hosts that should be excluded from the proxy, e.g. localhost,
            separated by a comma

        Returns
        -------
        ProxySettings
            The proxy settings object
        """
        parts = urlsplit(proxy_string)

        # Parse the protocol
        protocol_name = parts.scheme.lower()
        if protocol_name not in supported_proxy_protocols:
            raise ValueError(
                f"Invalid or unsupported protocol: {protocol_name}, to see all supported protocols, "
                f"call ProxySettings.supported_proxy_protocols()"
            )

        # Parse the user and password
        user_name = unquote_plus(parts.username) if parts.username else None
        password = unquote_plus(parts.password) if parts.password else None

        # Parse the host and port
        host_name = parts.hostname
        port_number = str(parts.port) if parts.port is not None else None

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
        """
        Return the supported proxy protocols for KNIME proxy settings in Python.

        Returns
        -------
        str
            A string containing the list of supported proxy protocols.

        """
        return f"""
        The KNIME proxy settings for python currently only support the following protocols:
        {supported_proxy_protocols}
        """

    @classmethod
    def from_dict(cls, proxy_dict):
        """
        Create a ProxySettings object from a dictionary.

        Parameters
        ----------
        proxy_dict : dict
            The dictionary containing the proxy settings.

        Returns
        -------
        ProxySettings
            The proxy settings object.
        """
        return cls(
            protocol_name=proxy_dict.get("protocol_name"),
            host_name=proxy_dict.get("host_name"),
            port_number=proxy_dict.get("port_number"),
            exclude_hosts=proxy_dict.get("exclude_hosts"),
            user_name=proxy_dict.get("user_name"),
            password=proxy_dict.get("password"),
        )


def get_proxy_settings(protocol_name: Optional[str] = None) -> Optional[ProxySettings]:
    """
    Get the proxy settings from the environment variables.

    Get the proxy settings as configured either in KNIME’s preferences or via environment variables.
    Even if the proxy settings were configured in KNIME’s preferences only, they are already set as
    environment variables for this Python process, so they are in effect for everything you do.

    Parameters
    ----------
    protocol_name : str
        The protocol name, e.g. 'http' or 'https'. To see all supported protocols,
        call ProxySettings.supported_proxy_protocols(). If not provided, the function will
        return the first proxy settings it finds.

    Returns
    -------
    ProxySettings
        The proxy settings object.
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

    exclude_hosts = os.environ.get(no_proxy_key, None)
    return ProxySettings.from_string(proxy_env_string, exclude_hosts)


def _set_proxy_settings(java_callback):
    """
    Set proxy settings to environment variable from Java callback.

    Parameters
    ----------
    java_callback : object
        The Java callback object. Must have the following methods:
        - get_global_proxy_list() -> JavaList[JavaMap[str, str]]
    """
    proxy_map_list = list(java_callback.get_global_proxy_list())
    for java_proxy_map in proxy_map_list:
        proxy_dict = dict(java_proxy_map)
        proxy_settings = ProxySettings.from_dict(proxy_dict)
        proxy_settings.set_as_environment_variable()


def _set_tmp_directory(java_callback):
    """
    Set temporary directory to environment variables and Python tempfile.tempdir from Java callback.

    Parameters
    ----------
    java_callback : object
        The Java callback object. Must have the following methods:
        - get_global_tmp_dir_path() -> String
    """
    tmp_directory = java_callback.get_global_tmp_dir_path()
    tempfile.tempdir = tmp_directory
    os.environ["TMPDIR"] = tmp_directory
    os.environ["TEMP"] = tmp_directory
    os.environ["TMP"] = tmp_directory


def _pathsep_join(first: Optional[str], second: str) -> str:
    """
    Helper method that joins the two paths using the os.pathsep separator, as needed for a PATH
    environment variable. If the first part is None, no separator is added
    """
    if first and second:
        return os.pathsep.join([first, second])
    elif first:
        return first
    else:
        return second


def _set_paths():
    """
    Make sure that the PATH variable includes the folder of the Python executable by appending
    that folder to the PATH (if needed). We append it to the end so we don't change the behavior
    of currently existing scripts where binaries from system paths would be picked up.

    Also set the LD_LIBRARY_PATH on linux and DYLD_FALLBACK_LIBRARY_PATH on macOS to the "lib"
    folder next to the "bin" folder that contains the Python executable if it exists, as we assume
    this to be a Python environment whose dynamic libraries should be loadable.

    On Windows, we add the possible locations for DLLs inside the environment to the PATH so that
    the system picks up DLLs in these folders, too.
    """
    # Get the directory containing the Python executable
    python_executable = sys.executable

    python_bin_dir = os.path.normpath(os.path.dirname(python_executable))

    # Add Python bin directory to PATH if not already present
    current_path = os.environ.get("PATH", "")
    path_entries = current_path.split(os.pathsep) if current_path else []

    # Normalize all path entries for comparison
    normalized_entries = [os.path.normpath(entry) for entry in path_entries]

    if python_bin_dir not in normalized_entries:
        # Append to the end to avoid changing existing behavior
        new_path = _pathsep_join(current_path, python_bin_dir)
        os.environ["PATH"] = new_path

    # Set library path for dynamic libraries
    python_parent_dir = os.path.dirname(python_bin_dir)

    system = platform.system().lower()
    if system == "windows":
        # On Windows, DLLs are loaded from the PATH, there's no dedicated
        # environment variable for DLLs.
        current_path = os.environ.get("PATH", "")

        for additional_dir in [os.path.join("Library", "bin"), "DLLs", "Scripts"]:
            additional_dir = os.path.join(python_parent_dir, additional_dir)
            if os.path.exists(additional_dir):
                current_path = _pathsep_join(current_path, additional_dir)

        os.environ["PATH"] = current_path
    else:
        # Look for a "lib" folder next to the "bin" folder
        lib_dir = os.path.join(python_parent_dir, "lib")

        if os.path.exists(lib_dir):
            if system == "linux":
                # Set LD_LIBRARY_PATH on Linux
                current_ld_path = os.environ.get("LD_LIBRARY_PATH", "")
                os.environ["LD_LIBRARY_PATH"] = _pathsep_join(current_ld_path, lib_dir)

            elif system == "darwin":  # macOS
                # Set DYLD_FALLBACK_LIBRARY_PATH on macOS
                current_dyld_path = os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", "")
                os.environ["DYLD_FALLBACK_LIBRARY_PATH"] = _pathsep_join(
                    current_dyld_path, lib_dir
                )
