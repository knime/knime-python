@echo off

rem %1: The conda directory.
rem %2: The environment name.
rem All subsequent arguments are passed to the Python process.

set knime_conda_directory=%1
set knime_conda_scripts_directory=%knime_conda_directory%\Scripts
set knime_conda_condabin_directory=%knime_conda_directory%\condabin
set knime_conda_environment=%2

set remaining_arguments=%*
call set remaining_arguments=%%remaining_arguments:*%2=%%

set PATH=%knime_conda_scripts_directory%;%knime_conda_condabin_directory%;%PATH%

rem Activate conda environment.
rem Try to use "conda.bat" by default which is the recommended way to call conda within a batch script.
rem If this fails, we fall back to, in this order, plain "activate" or the fully qualified "conda.bat" command.
set "knime_conda_activate_command="
call :conda_command_exists conda.bat
if %ERRORLEVEL% equ 0 (
	set knime_conda_activate_command=conda.bat activate
)
if not defined knime_conda_activate_command (
	call :conda_command_exists activate
	if %ERRORLEVEL% equ 0 (
		set knime_conda_activate_command=activate
	)
)
if not defined knime_conda_activate_command (
	set knime_conda_activate_command=%knime_conda_condabin_directory%\conda.bat activate
)
call %knime_conda_activate_command% %knime_conda_environment%

if %ERRORLEVEL% equ 0 (
	python %remaining_arguments%
) else (
	echo Failed to activate Conda environment. Please make sure Conda is properly installed and added to your PATH environment variable.
)

exit /b %ERRORLEVEL%

:conda_command_exists
where %~1 >nul 2>nul
exit /b %ERRORLEVEL%
