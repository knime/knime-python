# -*- coding: utf-8 -*-

import sys

_message = ''
_error = False


# check libs, output info and exit with 1 if an error occurred
def main():
    pythonVersion = sys.version_info
    print('Python version: ' + str(pythonVersion.major) + '.' + str(pythonVersion.minor) + '.' + str(pythonVersion.micro))
    check_required_libs()
    print(_message)
    if _error:
        sys.exit(1)


# check for all libs that are required by the python kernel
def check_required_libs():
    python3 = sys.version_info >= (3, 0)
    # these libs should be standard
    if python3:
        check_lib('io')
    else:
        check_lib('StringIO')
    check_lib('datetime')
    check_lib('math')
    check_lib('socket')
    check_lib('struct')
    check_lib('base64')
    check_lib('traceback')
    check_lib('os')
    check_lib('pickle')
    # these libs are non standard requirements
    check_lib('numpy')
    check_lib('pandas')
    check_lib('google.protobuf')


# checks if the given lib can be imported
def check_lib(lib):
    global _message, _error
    if not lib_available(lib):
        _message += 'Library ' + lib + ' is missing\n'
        _error = True


# returns true if the given library can successfully be imported, false otherwise
def lib_available(lib):
    local_env = {}
    exec('try:\n\timport ' + lib + '\n\tsuccess = True\nexcept:\n\tsuccess = False', {}, local_env)
    return local_env['success']


main()
