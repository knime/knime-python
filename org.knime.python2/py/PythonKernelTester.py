# -*- coding: utf-8 -*-

# ------------------------------------------------------------------------
#  Copyright by KNIME GmbH, Konstanz, Germany
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
#  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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

import sys

min_pandas_version = '0.20.0'
min_python_version = '2.7.0'
check_python_version_num = 2
additional_required_modules = []

_message = ''


# check libs, output info and exit with 1 if an error occurred
def main():
    parse_cmd_args()
    python_version = sys.version_info
    print('Python version: ' + str(python_version[0]) + '.' + str(python_version[1]) + '.' + str(python_version[2]))
    if python_version[0] != check_python_version_num:
        add_to_message('Python is required to have a major version of ' + str(check_python_version_num) + '. Use the options tab to choose the major version to use.')
    check_required_libs()
    print(_message)
    
# parse cmd arguments
def parse_cmd_args():
    global min_python_version, check_python_version_num, additional_required_modules
    min_python_version = sys.argv[1]
    check_python_version_num = int(min_python_version[0])
    mode = ''
    for i in range(2, len(sys.argv)):
        if sys.argv[i][0] == '-':
            mode = sys.argv[i]
        elif mode == '-m':
            additional_required_modules.append(sys.argv[i])
        else:
            raise ValueError('Could not process input arguments. Usage: \n python PythonKernelTester.py <version>\nOptional:\n-m\tlist of additional requierd modules (space separated)')


# check for all libs that are required by the python kernel
def check_required_libs():
    python3 = sys.version_info >= (3, 0)
    check_version_python()
    # these libs should be standard
    if python3:
        check_lib('io')
    else:
        check_lib('StringIO')
    check_lib('datetime', ['datetime'])
    check_lib('math')
    check_lib('socket')
    check_lib('struct')
    check_lib('base64')
    check_lib('traceback')
    check_lib('os')
    check_lib('pickle')
    check_lib('imp')
    check_lib('types')
    # these libs are non standard requirements
    check_lib('numpy')
    if check_lib('pandas', ['DataFrame'], min_pandas_version):
        check_version_pandas()
        
    for m in additional_required_modules:
        check_lib(m)


# check as specific library
# @param lib        the library's name
# @param cls        a list of classes to check for availability 
# @param version    the minimum library version (NOTE: version is not checked at the moment)
def check_lib(lib, cls=None, version=None):
    if cls is None:
        cls = []
    error = False
    if not lib_available(lib):
        error = True
        message = 'Library ' + lib + ' is missing'
        if version is not None:
            message += ', required minimum version is ' + version
        add_to_message(message)
    else:
        for cl in cls:
            if not class_available(lib, cl):
                error = True
                add_to_message('Class ' + cl + ' in library ' + lib + ' is missing')
    return not error


# check if a class is available from a specific library
# @param lib     the library's name
# @param cls     the class's name
def class_available(lib, cls):
    local_env = {}
    exec('try:\n\tfrom ' + lib + ' import ' + cls + '\n\tsuccess = True\nexcept:\n\tsuccess = False', {}, local_env)
    return local_env['success']


# returns true if the given library can successfully be imported, false otherwise
def lib_available(lib):
    local_env = {}
    exec('try:\n\timport ' + lib + '\n\tsuccess = True\nexcept:\n\tsuccess = False', {}, local_env)
    return local_env['success']

# check that the installed python version is >= min_python_version
def check_version_python():
    min_version = min_python_version.split('.')
    version = sys.version_info
    smaller = False
    for i in range(len(min_version)):
        if int(version[i]) > int(min_version[i]):
            break
        if int(version[i]) < int(min_version[i]):
            smaller = True
            break
    if smaller:
        add_to_message('Installed python version is ' + str(version[0]) + '.' + str(version[1]) + '.' + str(version[2])
                       + ', required minimum is ' + '.'.join(min_version))


# check that the installed pandas version is >= min_pandas_version
def check_version_pandas():
    min_version = min_pandas_version.split('.')
    try:
        import pandas
        version = pandas.__version__.split('.')
        if len(version) is not len(min_version):
            raise Exception()
        smaller = False
        for i in range(len(min_version)):
            if int(version[i]) > int(min_version[i]):
                break
            if int(version[i]) < int(min_version[i]):
                smaller = True
                break
        if smaller:
            add_to_message('Installed pandas version is ' + '.'.join(version) + ', required minimum is '
                           + '.'.join(min_version))
    except Exception:
        add_to_message('Could not detect pandas version, required minimum is ' + '.'.join(min_version))


# add a line to the output message
def add_to_message(line):
    global _message
    _message += line + '\n'


main()
