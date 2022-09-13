import textwrap
import unittest
import html

try:
    import markdown
    from knime_markdown_parser import KnimeMarkdownParser
except ModuleNotFoundError:
    raise unittest.SkipTest(
        "To enable tests for node description markdown parser you need to install markdown."
    )

import re

allowed_tags = [
    "br",
    "b",
    "i",
    "u",
    "tt",
    "ul",
    "ol",
    "a",
    "sup",
    "sub",
    "p",
    "hr",
    "pre",
    "h3",
    "h4",
    "table",
    "tr",
    "td",
]


class MarkdownDocstringTest(unittest.TestCase):
    """
    The custom knime MarkdownParser has a few different functionalities to match xml schema (http://knime.org/node/v4.1.xsd)
    The current version supports:
     - header down- and uprounding since only h3/h4 tags are allowed in default description
     - substitution of strong/em elements to b/i
     - table conversion without headers
    TODO:
    code to pre instead of code / pre
    """

    def setUp(self):
        self.parser = KnimeMarkdownParser()
        self._parser = markdown.Markdown(output_format="xhtml")

    def test_basic_header(self):
        ## Test header
        # ajdust up
        _s = "## HEADER"
        _expected = "<h3>HEADER</h3>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)
        _s = "### HEADER"
        _expected = "<h3>HEADER</h3>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)
        _s = "# HEADER"
        _expected = "<h3>HEADER</h3>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)
        _s = "# HEADER"

        # adjust down
        _s = "#### HEADER"
        _expected = "<h4>HEADER</h4>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)
        _s = "##### HEADER"
        _expected = "<h4>HEADER</h4>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)
        _s = "###### HEADER"
        _expected = "<h4>HEADER</h4>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)

    def test_line_header(self):
        _s = """
        Head
        ---
        """
        _expected = "<h4>Head</h4>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)

        _s = """
        Head
        ===
        """
        _expected = "<h3>Head</h3>"
        _s = self.parser.parse_basic(_s)
        self.assertEqual(_s, _expected)

    def test_basic_em_strong(self):

        s = "*strong*"
        #'<p><em>strong</em></p>'
        _expected = "<p><i>strong</i></p>"
        _s = self.parser.parse_basic(s)
        self.assertEqual(_expected, _s)

        s = "_strong_"
        #'<p><em>strong</em></p>'
        _expected = "<p><i>strong</i></p>"
        _s = self.parser.parse_basic(s)
        self.assertEqual(_expected, _s)

        s = "__strong__"
        _expected = "<p><b>strong</b></p>"
        _s = self.parser.parse_basic(s)
        self.assertEqual(_expected, _s)

        s = "*__strong__*"
        # "<p><em><b>strong</b></em></p>"
        _expected = "<p><i><b>strong</b></i></p>"
        _s = self.parser.parse_basic(s)
        self.assertEqual(_expected, _s)

        s = "**_strong_**"
        #'<p><strong><i>strong</i></strong></p>'
        _expected = "<p><b><i>strong</i></b></p>"
        _s = self.parser.parse_basic(s)
        self.assertEqual(_expected, _s)

        # TODO: dunno how to solve this one
        #       since nested / recursive tags fail
        # s = "**__strong__**"
        #'<p><strong><b>strong</b></strong></p>'
        # _expected = '<p><strong><b>strong</b></strong></p>'
        # _s = self.parser.parse_basic(s)
        # self.assertEqual(_expected, _s)

        # TODO: this fails because of nested tags
        # s = "_*simple*_"
        # _s = self.parser.parse_basic(s)
        # _html = "<p><i>simple</i></p>"
        # self.assertEqual(_html, _s)

        s = "__strong__"
        _expected = "<p><b>strong</b></p>"
        _s = self.parser.parse_basic(s)
        self.assertEqual(_expected, _s)

        s = "__so strong__"
        _s = self.parser.parse_basic(s)
        _html = "<p><b>so strong</b></p>"
        self.assertEqual(_html, _s)

        s = "__so strong*__"
        _s = self.parser.parse_basic(s)
        _html = "<p><b>so strong*</b></p>"
        self.assertEqual(_html, _s)

        s = "__*so* strong*__"
        _s = self.parser.parse_basic(s)
        _html = "<p><b><i>so</i> strong*</b></p>"
        self.assertEqual(_html, _s)

    def test_table(self):
        # test implemented table
        s = """
        | 1 | 2 | 3 |
        |---|---|---|
        | 1 | 3 | 3 |
        | 2 | 3 | 12|
        | 3 | 3 | 1 |
        """
        self.maxDiff = None
        _expected = """
        <table>
        <tr>
        <td><b>1</b></td>
        <td><b>2</b></td>
        <td><b>3</b></td>
        </tr>
        <tr>
        <td>1</td>
        <td>3</td>
        <td>3</td>
        </tr>
        <tr>
        <td>2</td>
        <td>3</td>
        <td>12</td>
        </tr>
        <tr>
        <td>3</td>
        <td>3</td>
        <td>1</td>
        </tr>
        </table>
        """
        _res = self.parser.parse_fulldescription(s)
        self.assertEqual(
            re.sub(r"\s", "", _res.strip()),
            re.sub(r"\s", "", textwrap.dedent(_expected).strip()),
        )

    def test_hr(self):
        desc = """

        ---
        """
        _res = self.parser.parse_fulldescription(desc)
        _expected = "<hr />"
        self.assertEqual(_expected, _res)

    def test_link(self):
        desc = "[link](https://www.example.com/my%20great%20page)"
        _expected = (
            '<p><a href="https://www.example.com/my%20great%20page">link</a></p>'
        )
        _res = self.parser.parse_basic(desc)

        self.assertEqual(_res, _expected)

    def test_html_escape(self):
        desc = '<test id="id"></test>'
        self.assertEqual(
            self.parser.parse_basic(desc),
            f"<p>{html.escape(desc)}</p>",
        )

    def test_pre_decoded(self):
        desc = """
        No Code
        
            Code<o>
        """
        _expected = "<p>No Code</p>\n<pre>\nCode<o>\n</pre>"
        self.assertEqual(self.parser.parse_basic(desc), textwrap.dedent(_expected))

    def test_tt(self):
        desc = """`Test`"""
        self.assertEqual(
            self.parser.parse_basic(desc),
            f"<p><tt>Test</tt></p>",
        )

        desc = """
        ``` Test ```
        """
        self.assertEqual(
            self.parser.parse_basic(desc),
            f"<p><tt>Test</tt></p>",
        )

    def test_line_break(self):
        desc = """
        break  
        hammertime
        """
        _expected = "<p>break<br />\nhammertime</p>"
        _res = self.parser.parse_basic(desc)
        self.assertEqual(_res, _expected)
