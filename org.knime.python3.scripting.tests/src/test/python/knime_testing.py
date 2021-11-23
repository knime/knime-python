from unittest import TextTestResult

from io import StringIO

# TODO: we could move this module to a more general plug-in if it proves to be useful for communicating Python unittest
#  results back to JUnit


class PythonTestResult:
    def __init__(self, test_result: TextTestResult):
        self._test_result = test_result

    def wasSuccessful(self) -> bool:
        return self._test_result.wasSuccessful()

    def getFailureReport(self) -> str:
        r = self._test_result
        original_stream = r.stream
        try:
            r.stream = _WritelnDecorator(StringIO())
            r.printErrors()
            # TextTestResult's printErrors does not print unexpected successes for some reason, so we do it ourselves.
            for test in r.unexpectedSuccesses:
                r.stream.writeln(r.separator1)
                r.stream.writeln(f"UNEXPECTED SUCCESS: {r.getDescription(test)}")
            return r.stream.getvalue()
        finally:
            r.stream = original_stream

    class Java:
        implements = ["org.knime.python3.scripting.PythonTestResult"]


class _WritelnDecorator(object):
    """
    Modified from unittest.runner._WritelnDecorator.
    """

    def __init__(self, stream):
        self.stream = stream

    def __getattr__(self, attr):
        return getattr(self.stream, attr)

    def writeln(self, arg=None):
        if arg:
            self.write(arg)
        self.write("\n")
