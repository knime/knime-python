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
@author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
@author Patrick Winter, KNIME GmbH, Konstanz, Germany
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

# This should be the first statement in each module (except for __future__ statements) that makes specific demands on
# the Python environment.
from numpy import string_
import EnvironmentHelper

if EnvironmentHelper.is_python3():
    from io import StringIO
else:
    from StringIO import StringIO

import abc
import os
import socket
import sys
import traceback
import warnings
import re

from debug_util import debug_msg

from Borg import Borg
from messaging import RequestHandlers
from PythonCommands import PythonCommands
from PythonUtils import Simpletype
from PythonUtils import invoke_safely
from PythonUtils import load_module_from_path
from PythonUtils import object_to_string
from Serializer import Serializer
from TypeExtensionManager import TypeExtensionManager
from AutocompletionUtils import disable_autocompletion

if EnvironmentHelper.is_jedi_available():
    import jedi


class PythonKernelBase(Borg):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(PythonKernelBase, self).__init__()

        self._is_running = False
        self._is_closed = False

        # Initialize workspace.
        self._exec_env = None
        self.reset()

        # These will be populated in start():
        # TCP connection.
        self._connection = None
        # Executors.
        self._execute_thread_executor = None
        self._executor = None
        # Commands/messaging system.
        self._commands = None
        # Type extensions.
        self._type_extension_manager = None

        # Will be populated in set_serialization_library():
        # Serialization library module.
        self._serialization_library = None
        self._serializer = None

        self._cleanup_object_names = set()

        if sys.getdefaultencoding() != "utf-8":
            warnings.warn(
                "Your default encoding is not 'utf-8'. You may experience errors with non ascii characters!"
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abc.abstractmethod
    def _create_execute_thread_executor(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_executor(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_messaging(self, connection):
        raise NotImplementedError()

    @property
    def execute_thread_executor(self):
        return self._execute_thread_executor

    @property
    def executor(self):
        return self._executor

    @property
    def type_extension_manager(self):
        return self._type_extension_manager

    @property
    def serializer(self):
        return self._serializer

    def register_task_handler(self, task_category, handler, executor=None):
        return self._commands.message_handlers.register_message_handler(
            task_category, self._commands.create_task_factory(handler, executor)
        )

    def unregister_task_handler(self, task_category):
        return self._commands.message_handlers.unregister_message_handler(task_category)

    def add_cleanup_object_name(self, variable_name):
        self._cleanup_object_names.add(variable_name)

    def remove_cleanup_object_name(self, variable_name):
        self._cleanup_object_names.remove(variable_name)

    def set_serialization_library(self, path_to_serialization_library_module):
        debug_msg("Load serialization library.")
        self._serialization_library = self._load_serialization_library(
            path_to_serialization_library_module
        )
        debug_msg("Create serialization helper.")
        self._serializer = Serializer(
            self._serialization_library, self._type_extension_manager
        )

    # Kernel commands:

    def put_variable(self, name, variable):
        """
        Put the given variable into the local environment under the given name.
        """
        self._exec_env[name] = variable

    def get_variable(self, name):
        """
        Get the variable with the given name.
        """
        if name in self._exec_env:
            return self._exec_env[name]
        else:
            raise NameError(name + " is not defined.")

    def get_variable_or_default(self, name, default):
        """
        Get the variable with the given name if available in the workspace or the default otherwise.
        """
        if name in self._exec_env:
            return self._exec_env[name]
        else:
            return default

    def list_variables(self):
        """
        List all currently loaded modules and defined classes, functions and variables.
        """
        # create lists of modules, classes, functions and variables
        modules = []
        classes = []
        functions = []
        variables = []
        # iterate over dictionary to and put modules, classes, functions and variables in their respective lists
        for key, value in dict(self._exec_env).items():
            # get name of the type
            var_type = type(value).__name__
            # class type changed from classobj to type in python 3
            class_type = "classobj"
            if EnvironmentHelper.is_python3():
                class_type = "type"
            if var_type == "module":
                modules.append({"name": key, "type": var_type, "value": ""})
            elif var_type == class_type:
                classes.append({"name": key, "type": var_type, "value": ""})
            elif var_type == "function":
                functions.append({"name": key, "type": var_type, "value": ""})
            elif key != "__builtins__":
                value = object_to_string(value)
                variables.append({"name": key, "type": var_type, "value": value})
        # sort lists by name
        modules = sorted(modules, key=lambda k: k["name"])
        classes = sorted(classes, key=lambda k: k["name"])
        functions = sorted(functions, key=lambda k: k["name"])
        variables = sorted(variables, key=lambda k: k["name"])
        # create response list and add contents of the other lists in the order they should be displayed
        response = []
        response.extend(modules)
        response.extend(classes)
        response.extend(functions)
        response.extend(variables)
        return response

    def append_to_table(self, name, data_frame):
        """
        Append the given data frame to an existing one, if it does not exist put the data frame into the local
        environment.
        """
        if self._exec_env[name] is None:
            self._exec_env[name] = data_frame
        else:
            self._exec_env[name] = self._exec_env[name].append(data_frame)

    @staticmethod
    def has_auto_complete():
        """
        Returns true if autocomplete is available, false otherwise.
        """
        return EnvironmentHelper.is_jedi_available()

    @staticmethod
    def is_inside_comment_or_string(current_line, cursor):
        """
        Returns True if the cursor position is within a single-line string or comment.

        Used as an indicator of whether to enable autocompletion at the current cursor position.
        """
        python_version = sys.version[0]
        # shift the cursor position to be 0-based
        cursor -= 1

        in_string = False
        in_comment = False

        quotation_mark_occurences = []
        string_start_end_indices = []
        f_string_start_indices = []
        curly_brace_start_end_indices = []

        # for each hashtag found, we have a boolean indicator of whether it's inside a string
        hashtag_indices = [
            (idx, False)
            for idx, char in enumerate(current_line[:cursor])
            if char == "#"
        ]

        regex_expressions = [
            ("'", r"(?<!\')\'(?!\')"),  # match only single quotes
            ('"', r'(?<!\")\"(?!\"")'),  # match only double quotes
            ("'''", r"\'{3}"),  # match only triple single quotes
            ('"""', r"\"{3}"),  # match only triple double quotes
        ]

        if python_version[0] != "2":
            # if Python version is 3.x, add an expression to match f-string start indices
            regex_expressions += [("f", r"""(?<=f)(\'|\")""")]

        # extract the indices of quotation marks (and f-string start indices if Python version is 3.x)
        for symbol, expr in regex_expressions:
            matches = re.finditer(expr, current_line)
            if symbol == "f":
                f_string_start_indices += [match.start(0) for match in matches]
            else:
                quotation_mark_occurences += [
                    (match.start(0), symbol) for match in matches
                ]

        # sort the collected quotation mark indices in order of appearance in the line
        quotation_mark_occurences.sort()
        string_opening_symbol = ""
        current_string_idx = 0
        for idx, symbol in quotation_mark_occurences:
            if string_opening_symbol == "":
                string_opening_symbol = symbol
                # if the current string is unclosed, save a -1 as its end index
                string_start_end_indices.append([idx, -1])
            else:
                if symbol == string_opening_symbol:
                    # found the end of the current string, replace the -1 with the current index
                    string_opening_symbol = ""
                    string_start_end_indices[current_string_idx][1] = idx
                    current_string_idx += 1

        # save indices of curly braces inside the discovered f-strings (if any)
        for f_string_idx in range(len(f_string_start_indices)):
            for string_idx in range(len(string_start_end_indices)):
                if (
                    string_start_end_indices[string_idx][0]
                    == f_string_start_indices[f_string_idx]
                ):
                    # match the start index of the current f-string with the start index of one of the found strings
                    # in order to get the end index of the f-string
                    start_idx = string_start_end_indices[string_idx][0]
                    end_idx = string_start_end_indices[string_idx][1]
                    substring = current_line[start_idx:end_idx]

                    opening_curly_braces = [
                        idx + start_idx
                        for idx, char in enumerate(substring)
                        if char == "{"
                    ]
                    closing_curly_braces = [
                        idx + start_idx
                        for idx, char in enumerate(substring)
                        if char == "}"
                    ]

                    # this covers the case of being inside an unfinished f-string, where there might be
                    # fewer closing curly braces than the opening ones. We pad the list of closing curly
                    # braces with -1's to be able to zip it with the list of the opening ones.
                    if len(opening_curly_braces) > len(closing_curly_braces):
                        closing_curly_braces += [-1] * (
                            len(opening_curly_braces) - len(closing_curly_braces)
                        )

                    curly_brace_start_end_indices += list(
                        zip(opening_curly_braces, closing_curly_braces)
                    )
                    break

        # check for the cursor and discovered hashtags being inside a string
        for start_idx, end_idx in string_start_end_indices:
            if not in_string or in_comment:
                if (end_idx == -1) or (start_idx < cursor < end_idx):
                    in_string = True
                    for (
                        curly_brace_start,
                        curly_brace_end,
                    ) in curly_brace_start_end_indices:
                        if (curly_brace_end == -1) and (
                            start_idx < curly_brace_start < cursor
                        ):
                            in_string = False
                        elif (
                            start_idx
                            < curly_brace_start
                            < cursor
                            < curly_brace_end
                            < end_idx
                        ):
                            in_string = False

                if not in_string and end_idx != -1:
                    for idx, (hashtag_idx, is_ignored) in enumerate(hashtag_indices):
                        # a hashtag is ignored if inside a string
                        if start_idx < hashtag_idx < end_idx:
                            is_ignored = True
                        hashtag_indices[idx] = (hashtag_idx, is_ignored)

        # we only check for being inside a comment if we are not already inside a string
        if not in_string:
            if len(string_start_end_indices) == 0:
                # if no strings were found, a single hashtag before the cursor means we are in a comment
                in_comment = len(hashtag_indices) > 0
            else:
                # otherwise we check if there is a False is_ignored indicator in the list of hashtags,
                # which means it isn't inside a string
                in_comment = False in [
                    is_ignored for idx, is_ignored in hashtag_indices
                ]
        return in_string or in_comment

    def auto_complete(self, source_code, line, column):
        """
        Returns a list of auto suggestions for the given code at the given cursor position.
        Skips producing suggestions if the cursor is within a comment or a string.

        Note that Jedi >=0.16.0 automatically disables autocompletion within single/multi-line strings,
        but not inside comments - this check is done manually.
        """
        response = []
        if self.has_auto_complete():
            # Needed to make jedi thread-safe. Calls to this method are initiated asynchronously on the Java side.
            jedi.settings.fast_parser = False
            try:
                # get possible completions by using Jedi and providing the source code and the cursor position
                # note: the line number gets incremented by 1 since Jedi's line numbering starts at 1.
                current_line = source_code.split("\n")[line][:column]
                line += 1
                if not disable_autocompletion(current_line):
                    # if not self.is_inside_comment_or_string(current_line, column):
                    try:
                        # try Jedi's 0.16.0+ API, otherwise fall back to the old API
                        completions = jedi.Script(source_code, path="").complete(
                            line,
                            column,
                        )
                    except AttributeError:
                        # fall back to Jedi's old API. ("complete" raises the AttributeError caught here.).
                        completions = jedi.Script(
                            source_code, line, column
                        ).completions()

                    # extract interesting information if autocomplete is enabled at the cursor position
                    for completion in completions:
                        if completion.name.startswith("_"):
                            # skip all private members
                            break
                        response.append(
                            {
                                "name": completion.name,
                                "type": completion.type,
                                "doc": completion.docstring(),
                            }
                        )
            except Exception:
                warnings.warn("An error occurred while autocompleting.")
        return response

    def execute(self, source_code, initiating_message_id=None):
        """
        Executes the given source code.
        """
        output = StringIO()
        error = StringIO()
        # Log outputs/errors(/warnings) to both stdout/stderr and variables. Note that we do _not_ catch any otherwise
        # uncaught exceptions here for the purpose of logging. That is, uncaught exceptions will regularly lead to the
        # exceptional termination of this method and need to be handled and possibly logged by the callers of this
        # method.
        regular_std_out = sys.stdout
        regular_std_err = sys.stderr
        sys.stdout = PythonKernelBase._Logger(sys.stdout, output)
        sys.stderr = PythonKernelBase._Logger(sys.stderr, error)

        # FIXME: This is dangerous!
        self._exec_env["python_messaging_initiating_message_id"] = initiating_message_id
        try:
            exec(source_code, self._exec_env, self._exec_env)
        finally:
            sys.stdout = regular_std_out
            sys.stderr = regular_std_err

        return [output.getvalue(), error.getvalue()]

    def reset(self):
        """
        Reset the current workspace.
        """
        self._exec_env = {"workspace": self}
        try:
            import knime_jupyter

            knime_jupyter.__implementation__._resolve_knime_url = (
                lambda url: self._commands.resolve_knime_url(url).get()
            )
            self._exec_env[knime_jupyter.__name__] = knime_jupyter
        except Exception:
            warnings.warn("Failed to initialize Jupyter notebook support.")

    # Life cycle:

    def start(self):
        if not self._is_running:
            if self._is_closed:
                raise RuntimeError("Python kernel is closed and cannot be restarted.")
            self._is_running = True
            debug_msg("Connect.")
            self._connection = self._connect(("localhost", int(sys.argv[1])))
            debug_msg("Create executors.")
            self._execute_thread_executor = self._create_execute_thread_executor()
            self._executor = self._create_executor()
            debug_msg("Create Python commands.")
            self._commands = PythonCommands(
                self._create_messaging(self._connection), self
            )
            self._setup_builtin_request_handlers()
            debug_msg("Create type extension manager.")
            self._type_extension_manager = TypeExtensionManager(self._commands)
            # Start commands/messaging system once everything is set up.
            debug_msg("Start Python commands.")
            self._commands.start()

    def close(self):
        if self._is_running and not self._is_closed:
            self._is_running = False
            self._is_closed = True
            # Order is intended.
            invoke_safely(None, lambda s: s._cleanup(), self)
            invoke_safely(None, lambda e: e.shutdown(wait=False), self._executor)
            invoke_safely(
                None, lambda e: e.shutdown(wait=False), self._execute_thread_executor
            )
            invoke_safely(None, lambda c: c.close(), self._commands)
            invoke_safely(
                None, lambda c: c.shutdown(socket.SHUT_RDWR), self._connection
            )
            invoke_safely(None, lambda c: c.close(), self._connection)

    # Helper:

    @staticmethod
    def _connect(parameters):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect(parameters)
        return connection

    def _setup_builtin_request_handlers(self):
        request_handlers = RequestHandlers.get_builtin_request_handlers()
        for message_category, handler in request_handlers.items():
            self.register_task_handler(message_category, handler)

    @staticmethod
    def _load_serialization_library(serializer_path):
        last_separator = serializer_path.rfind(os.sep)
        serializer_directory_path = serializer_path[0 : last_separator + 1]
        sys.path.append(serializer_directory_path)
        serializer = load_module_from_path(serializer_path)
        serializer.init(Simpletype)
        return serializer

    def _cleanup(self):
        """
        Is called on shutdown to clean up all variables whose names are registered in _cleanup_object_names.
        """
        if self._cleanup_object_names is not None:
            for name in self._cleanup_object_names:
                obj = self.get_variable(name)
                if obj is not None:
                    try:
                        self._cleanup_object(obj, name)
                    except BaseException:
                        pass

    def _cleanup_object(self, obj, obj_name):
        obj._cleanup()

    # Logging:

    class _Logger(object):
        """
        Logger class for parallel logging to sys.stdout and a sink, e.g. a StringIO object or a file.
        """

        def __init__(self, stdstream, sink):
            self.stdstream = stdstream
            self.sink = sink

        def write(self, message):
            self.stdstream.write(message)
            self.sink.write(message)

        def writelines(self, sequence):
            self.stdstream.writelines(sequence)
            self.sink.writelines(sequence)

        def flush(self):
            self.stdstream.flush()
            self.sink.flush()

        def isatty(self):
            return False
