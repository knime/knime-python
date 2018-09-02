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

from __future__ import print_function

import sys
import traceback

_DEBUG_ENABLED = False
_BREAKPOINTS_ENABLED = False
_DEBUG_LOG_ENABLED = False
_DEBUG_LOG_FILE = None
_LOG_TO_STDERR = False


def is_debug_enabled():
    global _DEBUG_ENABLED
    return _DEBUG_ENABLED


def init_debug(enable_breakpoints=True, enable_debug_log=True, debug_log_to_stderr=False):
    if is_debug_enabled():
        return

    try:
        # For more information. please see http://pydev.org/manual_adv_remote_debugger.html.
        # You have to create and export a new environment variable PYTHONPATH that points to the pysrc folder located in
        # <eclipse>\plugins\org.python.pydev.core_xxx. (E.g. within a startup script that calls the Python binary.)
        import pydevd  # With the addon script.module.pydevd, only use `import pydevd`.
    except ImportError as ex:
        import sys
        sys.stderr.write(("Error '{0}': " +
                          "You must add org.python.pydev.debug.pysrc to your PYTHONPATH.").format(ex))
        sys.exit(1)

    # stdoutToServer and stderrToServer redirect stdout and stderr to eclipse console.
    # pydevd.settrace('localhost', port=5678, suspend=False, stdoutToServer=True, stderrToServer=True)

    global _DEBUG_ENABLED
    _DEBUG_ENABLED = True

    global _BREAKPOINTS_ENABLED
    _BREAKPOINTS_ENABLED = enable_breakpoints

    global _DEBUG_LOG_ENABLED
    _DEBUG_LOG_ENABLED = enable_debug_log

    global _LOG_TO_STDERR
    _LOG_TO_STDERR = debug_log_to_stderr

    _write_debug_message('Python kernel enabled debugging. '
                         + 'Breakpoints are ' + ('enabled' if _BREAKPOINTS_ENABLED else 'disabled') + '. '
                         + 'Debug log is ' + ('enabled' if _DEBUG_LOG_ENABLED else 'disabled') + '.',
                         _get_output_file())


def breakpoint():
    global _BREAKPOINTS_ENABLED
    if not (is_debug_enabled() and _BREAKPOINTS_ENABLED):
        return

    import pydevd
    pydevd.settrace('localhost', port=5678, suspend=True, stdoutToServer=True, stderrToServer=True)
    import threading
    threading.settrace(pydevd.GetGlobalDebugger().trace_dispatch)


def debug_msg(message, exc_info=False):
    global _DEBUG_LOG_ENABLED
    if not (is_debug_enabled() and _DEBUG_LOG_ENABLED):
        return

    file = _get_output_file()
    _write_debug_message(message, file)
    if exc_info:
        traceback.print_exc(file=file)


def _write_debug_message(message, file):
    print(message, file=file)  # 'flush' keyword argument is not supported by Python 2.
    file.flush()


def _get_output_file():
    global _LOG_TO_STDERR
    return sys.stderr if _LOG_TO_STDERR else sys.stdout
