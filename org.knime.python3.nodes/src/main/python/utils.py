"""
A collection of small utilities for the Pure-Python node extensions.
"""

from collections import namedtuple


def parse(s):
    """
    Parses a string of the form "0.1.2" into a Version namedtuple, which can then be compared with other Version objects.
    """
    major, minor, patch = s.split(".")
    return Version(major, minor, patch)


Version = namedtuple("Version", ["major", "minor", "patch"])
