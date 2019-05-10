@SET PATH=C:\tools\Anaconda3\Scripts;%PATH%

@CALL activate py36_knime || ECHO Activating py36_knime failed
@python %*
