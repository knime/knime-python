"""
A collection of small utilities for the Pure-Python node extensions.
"""

from collections import namedtuple


def parse(s):
    """
    Parses a string of the form "0.1.2" into a Version namedtuple, which can then be compared with other Version objects.

    If the provided string is None, then the Version is set to "0.0.0".
    If the provided string is not of the correct format, then a ValueError is raised.
    """
    if s is None:
        return Version("0", "0", "0")

    try:
        major, minor, patch = s.split(".")
        return Version(major, minor, patch)
    except ValueError:
        raise ValueError(f"Incorrect version format: {s}. Must be of the form '0.1.2'.")


class Version(namedtuple("Version", ["major", "minor", "patch"])):
    """
    A Version namedtuple with a __repr__ method that returns a string of the form "0.1.2".
    """

    def __repr__(self):
        return f"{self.major}.{self.minor}.{self.patch}"

    def __str__(self):
        return self.__repr__()
