@echo off

rem %1: The conda directory.
rem %2: The environment name.
rem All subsequent arguments are passed to the Python process.

set knime_conda_directory=%1\Scripts
set knime_conda_environment=%2

set remaining_arguments=%*
call set remaining_arguments=%%remaining_arguments:*%2=%%

set PATH=%knime_conda_directory%;%PATH%
call conda.bat activate %knime_conda_environment% || ECHO Failed to activate Conda environment
python %remaining_arguments%