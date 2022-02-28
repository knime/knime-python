# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
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
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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

"""
A collection of functions to support code autocompletion in Python scripts
by checking if the cursor is within a string or comment.

Note that this only handles single-line strings. Multi-line string detection
is handled by Jedi itself (from version 0.16.0+).

@author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
"""

import re
import sys


def get_quote_indices(line):
    """
    Looks for occurences of various types of quotation marks in the current line
    by matching with the corresponding regex expression.

    Parameters:
    line (str): the line of code from which autocompletion was invoked.

    Returns:
    quote_indices (list): list of tuples (index of symbol in line, symbol).
    """
    quote_indices = []
    regex_expressions = [
        ("'", r"(?<!\')\'(?!\')"),  # match only single quotes
        ('"', r"(?<!\")\"(?!\")"),  # match only double quotes
        ("'''", r"\'{3}"),  # match only triple single quotes
        ('"""', r"\"{3}"),  # match only triple double quotes
    ]

    # extract the indices of quotation marks
    for symbol, expr in regex_expressions:
        matches = re.finditer(expr, line)
        quote_indices += [(match.start(0), symbol) for match in matches]

    return sorted(quote_indices)


def get_f_string_start_indices(line):
    """
    Looks for occurences of f-strings in the current line
    by matching with the corresponding regex expression.

    Parameters:
    line (str): the line of code from which autocompletion was invoked.

    Returns:
    f_string_start_indices (list): list of start positions of f-strings.
    """
    regex_expression = r"""(?<=f)(\'|\")"""
    matches = re.finditer(regex_expression, line)
    f_string_start_indices = [match.start(0) for match in matches]

    return f_string_start_indices


def get_string_indices(line):
    """
    Processes the list of quotation marks found in the current line to
    find the start and end indices of each string (if any).

    Parameters:
    line (str): the line of code from which autocompletion was invoked.

    Returns:
    string_indices (list): list of tuples containing the start and end indices of each string found in the line.
    """
    # sort the collected quotation mark indices in order of appearance in the line
    quote_indices = get_quote_indices(line)
    string_indices = []

    string_opening_symbol = ""
    for idx, symbol in quote_indices:
        if string_opening_symbol == "":
            # entered a new string, set its start and end indices
            string_opening_symbol = symbol
            string_indices += [[idx, -1]]
        elif symbol == string_opening_symbol:
            # found the end of the current string, set the end index with the current index
            string_opening_symbol = ""
            string_indices[-1][1] = idx

    return string_indices


def get_replacement_field_indices(line, string_indices):
    """
    Looks for replacement fields (the areas enclosed by curly braces inside f-strings)
    in the strings found in the current line.

    Parameters:
    line (str): the line of code from which autocompletion was invoked.
    string_indices (list): list of tuples containing the start and end indices of each string found in the line.

    Returns:
    replacement_field_indices (list): list of tuples (opening curly brace index, closing curly brace index)
    """
    f_string_start_indices = get_f_string_start_indices(line)
    replacement_field_indices = []

    # save indices of curly braces inside the discovered f-strings (if any)
    for f_string_idx in range(len(f_string_start_indices)):
        for string_idx in range(len(string_indices)):
            if string_indices[string_idx][0] == f_string_start_indices[f_string_idx]:
                # match the start index of the current f-string with the start index of one of the
                # found strings in order to get the end index of the f-string
                start_idx = string_indices[string_idx][0]
                end_idx = string_indices[string_idx][1]

                f_string = line[start_idx:end_idx]

                opening_curly_braces = [
                    idx + start_idx for idx, char in enumerate(f_string) if char == "{"
                ]
                closing_curly_braces = [
                    idx + start_idx for idx, char in enumerate(f_string) if char == "}"
                ]

                if len(opening_curly_braces) > len(closing_curly_braces):
                    # found fewer closing braces than opening ones, hence we are in an unfinished f-string.
                    # pad the list of closing braces to allow zipping with the list of opening braces.
                    # -1 signifies that the replacement field is not closed
                    closing_curly_braces += [-1] * (
                        len(opening_curly_braces) - len(closing_curly_braces)
                    )

                replacement_field_indices += list(
                    zip(opening_curly_braces, closing_curly_braces)
                )
                # we can stop the loop and move onto the next f-string
                break

    return replacement_field_indices


def get_hashtag_indices(line, string_indices):
    """
    Locates hashtags in the current line and checks if they are inside a string.

    Parameters:
    line (str): the line of code from which autocompletion was invoked.
    string_indices (list): list of tuples containing the start and end indices of each string found in the line.

    Returns:
    hashtag_indices (list): list of tuples (hashtag index, boolean of whether the hashtag is in a string).
    """
    hashtag_indices = [(idx, False) for idx, char in enumerate(line) if char == "#"]

    for start_idx, end_idx in string_indices:
        for idx, (hashtag_idx, is_in_string) in enumerate(hashtag_indices):
            if not is_in_string:
                if end_idx != -1:
                    is_in_string = True if start_idx < hashtag_idx < end_idx else False
                else:
                    is_in_string = True if start_idx < hashtag_idx else False
                hashtag_indices[idx] = (hashtag_idx, is_in_string)

    return hashtag_indices


def parse_line(line):
    """
    Parses the current line of code to find strings, f-string replacement fields (Python 3.6+),
    and hashtag symbols.

    Parameters:
    line (str): the line of code from which autocompletion was invoked.

    Returns:
    string_indices (list): list of tuples containing the start and end indices of each string found in the line.
    replacement_field_indices (list): list of tuples (opening curly brace index, closing curly brace index).
    hashtag_indices (list): list of tuples (hashtag index, boolean of whether the hashtag is in a string).
    """
    python_version = sys.version[:3]

    string_indices = get_string_indices(line)
    if python_version[0] != 2:
        replacement_field_indices = get_replacement_field_indices(line, string_indices)
    else:
        # f-strings only supported by Python 3.6+
        replacement_field_indices = []
    hashtag_indices = get_hashtag_indices(line, string_indices)

    return string_indices, replacement_field_indices, hashtag_indices


def is_cursor_in_string(string_indices, replacement_field_indices):
    """
    Checks whether the cursor position is within any of the discovered strings.
    If the cursor is inside any of the replacement fields inside an f-string, it is not inside a string.

    Parameters:
    string_indices (list): list of tuples containing the start and end indices of each string found in the line.
    replacement_field_indices (list): list of tuples (opening curly brace index, closing curly brace index)

    Returns:
    in_string (bool): a boolean indicating whether the cursor is inside a string.
    """
    in_string = False
    for start_idx, end_idx in string_indices:
        if not in_string and end_idx == -1:
            # the -1 indicates that the current string is unfinished
            in_string = True
            # check if the cursor is in a replacement field
            for curly_brace_start, curly_brace_end in replacement_field_indices:
                if start_idx < curly_brace_start and curly_brace_end == -1:
                    in_string = False
                    break
            break

    return in_string


def is_cursor_in_comment(in_string, string_indices, hashtag_indices):
    """
    If the cursor is not already inside a string, checks if the current line has hashtags outside of a string.

    Parameters:
    in_string (bool): a boolean indicating whether the cursor is inside a string.
    string_indices (list): list of tuples containing the start and end indices of each string found in the line.
    hashtag_indices (list): list of tuples (hashtag index, boolean of whether the hashtag is in a string).

    Returns:
    in_comment (bool): a boolean indicating whether the cursor is inside a comment.
    """
    in_comment = False
    # we only check for being inside a comment if we are not already inside a string
    if not in_string:
        if len(string_indices) == 0:
            # if no strings were found, a single hashtag before the cursor means we are in a comment
            in_comment = len(hashtag_indices) > 0
        else:
            # otherwise we check if there is a False is_in_string indicator in the list of hashtags
            in_comment = False in [
                is_in_string for idx, is_in_string in hashtag_indices
            ]

    return in_comment


def disable_autocompletion(current_line):
    """
    Returns True if the cursor position is within a single-line string or comment.
    Used as an indicator of whether to enable autocomplete at the current cursor position.

    Parameters:
    current_line (str): the line of code from which autocompletion was invoked.

    Returns:
    in_string (bool): a boolean indicating whether the cursor is inside a string.
    in_comment (bool): a boolean indicating whether the cursor is inside a comment.
    """
    string_indices, replacement_field_indices, hashtag_indices = parse_line(
        current_line
    )

    in_string = is_cursor_in_string(string_indices, replacement_field_indices)
    in_comment = is_cursor_in_comment(in_string, string_indices, hashtag_indices)

    return in_string or in_comment
