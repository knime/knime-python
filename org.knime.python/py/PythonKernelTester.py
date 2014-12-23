# -*- coding: utf-8 -*-

import sys

min_pandas_version = '0.7.0'
min_python_version = '2.5.0'
min_protobuf_version = '2.5.0'

_message = ''


# check libs, output info and exit with 1 if an error occurred
def main():
    pythonVersion = sys.version_info
    print('Python version: ' + str(pythonVersion[0]) + '.' + str(pythonVersion[1]) + '.' + str(pythonVersion[2]))
    check_required_libs()
    print(_message)


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
    if check_lib('google.protobuf', version=min_protobuf_version):
        check_version_protobuf()


def check_lib(lib, cls=[], version=None):
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


def class_available(lib, cls):
    local_env = {}
    exec('try:\n\tfrom ' + lib + ' import ' + cls + '\n\tsuccess = True\nexcept:\n\tsuccess = False', {}, local_env)
    return local_env['success']

# returns true if the given library can successfully be imported, false otherwise
def lib_available(lib):
    local_env = {}
    exec('try:\n\timport ' + lib + '\n\tsuccess = True\nexcept:\n\tsuccess = False', {}, local_env)
    return local_env['success']


def check_version_python():
    min_version = min_python_version.split('.')
    version = sys.version_info
    smaller = False
    bigger = False
    for i in range(len(min_version)):
        if int(version[i]) > int(min_version[i]):
            bigger = True
            break
        if not bigger and int(version[i]) < int(min_version[i]):
            smaller = True
            break
    if smaller:
        add_to_message('Installed python version is ' + str(version[0]) + '.' + str(version[1]) + '.' + str(version[2]) + ', required minimum is ' + '.'.join(min_version))


def check_version_pandas():
    min_version = min_pandas_version.split('.')
    try:
        import pandas
        version = pandas.__version__.split('.')
        if len(version) is not len(min_version):
            raise Exception()
        smaller = False
        bigger = False
        for i in range(len(min_version)):
            if int(version[i]) > int(min_version[i]):
                bigger = True
                break
            if not bigger and int(version[i]) < int(min_version[i]):
                smaller = True
                break
        if smaller:
            add_to_message('Installed pandas version is ' + '.'.join(version) + ', required minimum is ' + '.'.join(min_version))
    except:
        add_to_message('Could not detect pandas version, required minimum is ' + '.'.join(min_version))


def check_version_protobuf():
    try:
        import table_pb2
    except Exception as e:
        add_to_message('Error while trying to load protobuf:\n' + str(e) + '\nThe minimum required protobuf version is ' + min_protobuf_version)


def add_to_message(line):
    global _message
    _message += line + '\n'


main()
