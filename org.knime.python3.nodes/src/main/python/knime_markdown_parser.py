import markdown
import textwrap
import re
import html

from markdown.inlinepatterns import (
    EmStrongItem,
    AsteriskProcessor,
)
from markdown.extensions import Extension

from markdown.preprocessors import Preprocessor
from markdown.postprocessors import Postprocessor
from markdown.blockprocessors import BlockProcessor
from markdown.extensions import Extension

from markdown.inlinepatterns import (
    EMPHASIS_RE,
    STRONG_RE,
    SMART_STRONG_RE,
    SMART_STRONG_EM_RE,
    SMART_EMPHASIS_RE,
    EM_STRONG_RE,
    EM_STRONG2_RE,
    STRONG_EM_RE,
    STRONG_EM2_RE,
    STRONG_EM3_RE,
)


class _HeaderPreprocessor(Preprocessor):
    """
    Currently in Description only h3/h4 tags are allowed.
    """

    def run(self, lines):
        new_lines = []
        for line in lines:
            # replace with h4
            if re.search("#{4,} ", line):
                line = re.sub("#+ ", "####", line)
                new_lines.append(line)
            elif re.search("#{1,3} ", line):
                line = re.sub("#+ ", "###", line)
                new_lines.append(line)
            else:
                new_lines.append(line)
        return new_lines


class _HTMLEncodePreprocessor(Preprocessor):
    def run(self, lines):
        return [html.escape(line) for line in lines]


class _KnimeProcessorAsterisk(AsteriskProcessor):
    PATTERNS = [
        EmStrongItem(re.compile(EM_STRONG_RE, re.DOTALL | re.UNICODE), "double", "b,i"),
        EmStrongItem(re.compile(STRONG_EM_RE, re.DOTALL | re.UNICODE), "double", "b,i"),
        EmStrongItem(
            re.compile(STRONG_EM3_RE, re.DOTALL | re.UNICODE), "double2", "b,i"
        ),
        EmStrongItem(re.compile(STRONG_RE, re.DOTALL | re.UNICODE), "single", "b"),
        EmStrongItem(re.compile(EMPHASIS_RE, re.DOTALL | re.UNICODE), "single", "i"),
    ]


class _KnimeProcessorUnderscore(AsteriskProcessor):

    PATTERNS = [
        EmStrongItem(
            re.compile(EM_STRONG2_RE, re.DOTALL | re.UNICODE), "double", "b,i"
        ),
        EmStrongItem(
            re.compile(STRONG_EM2_RE, re.DOTALL | re.UNICODE), "double", "i,b"
        ),
        EmStrongItem(
            re.compile(SMART_STRONG_EM_RE, re.DOTALL | re.UNICODE), "double2", "b,i"
        ),
        EmStrongItem(
            re.compile(SMART_STRONG_RE, re.DOTALL | re.UNICODE), "single", "b"
        ),
        EmStrongItem(
            re.compile(SMART_EMPHASIS_RE, re.DOTALL | re.UNICODE), "single", "i"
        ),
    ]


class _KnimeTable(Postprocessor):
    def run(self, text):
        text = re.sub(r"<tbody>", "", text)
        text = re.sub(r"</tbody>", "", text)
        text = re.sub(r"<thead>", "", text)
        text = re.sub(r"</thead>", "", text)
        text = re.sub(r"<th>", "<td><b>", text)
        text = re.sub(r"</th>", "</b></td>", text)
        return text


class _KnimePostHeader(Postprocessor):
    # AP-19260
    def run(self, text):
        text = re.sub(r"<h1>", "<h3>", text)
        text = re.sub(r"</h1>", "</h3>", text)
        text = re.sub(r"<h2>", "<h4>", text)
        text = re.sub(r"</h2>", "</h4>", text)
        return text


class _KnimePostCode(Postprocessor):
    def run(self, text):
        text = re.sub(r"<pre><code>", "<pre>", text)
        text = re.sub(r"</code></pre>", "</pre>", text)
        text = re.sub(r"<code>", "<tt>", text)
        text = re.sub(r"</code>", "</tt>", text)
        return text


class _KnExtension(Extension):
    """
    Basic extension for Knime schema.
    """

    def extendMarkdown(self, _md) -> None:
        # Preprocessors
        _md.preprocessors.register(_HeaderPreprocessor(), "headerlines", 100)
        _md.preprocessors.register(_HTMLEncodePreprocessor(), "html_encode", 100)

        # Remove not supported extensions
        _md.inlinePatterns.deregister("image_reference")
        _md.inlinePatterns.deregister("image_link")
        _md.inlinePatterns.deregister("short_image_ref")
        _md.inlinePatterns.deregister("autolink")
        _md.inlinePatterns.deregister("automail")

        # Register
        _md.inlinePatterns.register(_KnimeProcessorAsterisk(r"\*"), "i_strong", 110)
        _md.inlinePatterns.register(_KnimeProcessorUnderscore(r"_"), "strong_i", 100)

        #
        _md.postprocessors.register(_KnimeTable(), "knime_table", 200)
        _md.postprocessors.register(_KnimePostHeader(), "knime_post_headder", 200)
        _md.postprocessors.register(_KnimePostCode(), "knime_post_code", 200)


class KnimeMarkdownParser:
    """ """

    def __init__(self):
        self.md = markdown.Markdown(
            extensions=[_KnExtension(), "sane_lists", "tables"],
            output_format="xhtml",
        )

        # TODO headerlines
        self.md_basic = markdown.Markdown(
            extensions=[_KnExtension(), "sane_lists"],
            output_format="xhtml",
        )
        # self.md_basic.preprocessors.deregister("headerlines")

    def parse_fulldescription(self, doc):
        doc = textwrap.dedent(doc)
        return self.md.convert(doc)

    def parse_basic(self, doc):
        try:
            assert doc
            doc = textwrap.dedent(doc)
            return self.md_basic.convert(doc)
        except AssertionError:
            return "<i>No description available.</i>"

    def parse_ports(self, ports):
        return [
            {
                "name": port.name,
                "description": self.parse_basic(port.description),
            }
            for port in ports
        ]

    def parse_options(self, options):
        return [
            {
                "name": option["name"],
                "description": self.parse_basic(option["description"]),
            }
            for option in options
        ]

    def parse_tabs(self, tabs):
        return [
            {
                "name": tab["name"],
                "description": self.parse_basic(tab["description"]),
                "options": self.parse_options(tab["options"]),
            }
            for tab in tabs
        ]
