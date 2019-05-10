@SET PATH=C:\tools\Anaconda3\Scripts;%PATH%

@CALL activate py27_knime || ECHO Activating py27_knime failed
@python %*
