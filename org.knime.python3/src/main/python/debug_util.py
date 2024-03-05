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
Module encapsulating functionality used for debugging.
See: http://www.pydev.org/manual_adv_remote_debugger.html on how to setup debugging.

@author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
@author Patrick Winter, KNIME GmbH, Konstanz, Germany
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

import traceback

import inspect
import sys

_debug_enabled = False
_breakpoints_enabled = False
_debug_log_enabled = False
_debug_log_file = None
_log_to_stderr = False
_port = None


def is_debug_enabled():
    """
    Check if debug mode is enabled.

    Returns
    -------
    bool
        True if debug mode is enabled, False otherwise.
    """
    global _debug_enabled
    return _debug_enabled


def init_debug(
    enable_breakpoints=True, enable_debug_log=True, debug_log_to_stderr=False, port=5678
):
    """
    Initialize the debugging environment.

    Parameters
    ----------
    enable_breakpoints : bool, optional
        Whether to enable breakpoints or not. Default is True.
    enable_debug_log : bool, optional
        Whether to enable debug log or not. Default is True.
    debug_log_to_stderr : bool, optional
        Whether to log debug messages to stderr or not. Default is False.
    port : int, optional
        The port number for communication. Default is 5678.

    Raises
    ------
    ImportError
        If pydevd is not installed in the environment.


    Notes
    -----
    - This function initializes the debugging environment.
    - It sets the global variables to enable debugging.
    - It also checks if pydevd is installed and raises an error if not.
    - Debug log messages are written to the output file.

    Examples
    --------
    >>> init_debug()
    Python enabled debugging. Breakpoints are enabled. Debug log is enabled. Communicating via port 5678.
    """
    if is_debug_enabled():
        return

    global _debug_enabled
    _debug_enabled = True

    global _breakpoints_enabled
    _breakpoints_enabled = enable_breakpoints

    global _debug_log_enabled
    _debug_log_enabled = enable_debug_log

    global _log_to_stderr
    _log_to_stderr = debug_log_to_stderr

    global _port
    _port = port

    try:
        # For more information, please see http://pydev.org/manual_adv_remote_debugger.html.
        import pydevd
    except ImportError as ex:
        _write_debug_message(
            (
                f"Error '{ex}' in debug_util: You must have pydevd installed in your environment (e.g. via pip install "
                f"pydevd) before you can use this module for debugging."
            ),
            _get_output_file(),
        )
        raise

    _write_debug_message(
        "Python enabled debugging. Breakpoints are "
        + ("enabled" if _breakpoints_enabled else "disabled")
        + ". Debug log is "
        + ("enabled" if _debug_log_enabled else "disabled")
        + f". Communicating via port {_port}.",
        _get_output_file(),
    )


def breakpoint():
    """
    Enable a breakpoint for debugging.

    This function enables a breakpoint for debugging, if both the debugging feature and breakpoints are enabled. It uses the PyDev module to set a trace that allows for breakpoints to be triggered.

    Raises
    ------
    ConnectionRefusedError
        If the connection to the debugger is refused, indicating that the debugger is not running or the connection is blocked by a firewall.
    """
    global _breakpoints_enabled
    if not (is_debug_enabled() and _breakpoints_enabled):
        return

    import pydevd

    try:

        def do_nothing():
            # See below.
            pass

        global _port
        pydevd.settrace(
            stdout_to_server=True,
            stderr_to_server=True,
            port=_port,
            suspend=True,
            stop_at_frame=inspect.currentframe().f_back,
        )
        # We need to perform some operation here for some reason, otherwise stepping from the breakpoint won't work (at
        # least when using PyCharm).
        do_nothing()
    except ConnectionRefusedError as ex:
        raise ConnectionRefusedError(
            (
                "Connection to debugger refused. Please make sure to have the external debugger started and listening. "
                "Also make sure that the connection is not blocked by a firewall."
            )
        ) from ex


def debug(message, exc_info=False):
    """
    Log a debug message.

    Parameters
    ----------
    message : str
        The debug message to be logged.
    exc_info : bool, optional
        If True, print the traceback information along with the debug message (default is False).

    """
    global _debug_log_enabled
    if not (is_debug_enabled() and _debug_log_enabled):
        return

    file = _get_output_file()
    _write_debug_message(message, file)
    if exc_info:
        traceback.print_exc(file=file)


def _write_debug_message(message, file):
    """
    Write a debug message to a file.

    Parameters
    ----------
    message : str
        The message to be written to the file.

    file : _io.TextIOWrapper
        The file object to write the message to.

    """
    print(message, file=file, flush=True)


def _get_output_file():
    """
    Get the output file for logging messages.

    Returns
    -------
    file
        The output file for logging messages. If `_log_to_stderr` is True, returns `sys.stderr`. Otherwise, returns `sys.stdout`.
    """
    global _log_to_stderr
    return sys.stderr if _log_to_stderr else sys.stdout
