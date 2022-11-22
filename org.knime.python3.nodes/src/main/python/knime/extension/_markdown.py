from typing import Optional
import markdown
import re
import html
import sys

from markdown.inlinepatterns import (
    EmStrongItem,
    AsteriskProcessor,
)
from markdown.extensions import Extension

from markdown.preprocessors import Preprocessor
from markdown.postprocessors import Postprocessor
from markdown.treeprocessors import Treeprocessor
from markdown.extensions import Extension

from xml.etree.ElementTree import Element

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


class _HeadingPreprocessor(Preprocessor):
    """
    Currently in Description only h3/h4 tags are allowed.
    """

    def run(self, lines):
        new_lines = []
        for line in lines:
            # replace with h4
            if re.search("#{4,} ", line):
                line = re.sub("#+ ", "####", line)
            elif re.search("#{1,3} ", line):
                line = re.sub("#+ ", "###", line)
            new_lines.append(line)
        return new_lines


class _RemoveHeadingsPreprocessor(Preprocessor):
    """
    In Markdown, dashes can indicate either a heading if the previous line is
    non-empty (--+), or a horizontal rule otherwise (---+). We need to remove
    the former and keep the latter.
    """

    def run(self, lines):
        new_lines = []
        previous_line = ""
        for i, line in enumerate(lines):
            if i > 0:
                previous_line = lines[i - 1]

            if re.search("#{1,6} ", line):
                line = re.sub("#+ ", "", line)
            elif previous_line != "" and re.search("^(==+|--+)$", line):
                continue
            new_lines.append(line)

        return new_lines


class _HTMLTreeprocessor(Treeprocessor):
    @staticmethod
    def _unescape(s, quote=True):
        """
        symmetric html.escape
        """
        s = s.replace("&amp;", "&")  # Must be done first!
        s = s.replace("&lt;", "<")
        s = s.replace("&gt;", ">")
        if quote:
            s = s.replace("&quot;", '"')
            s = s.replace("&#x27;", "'")
        return s

    @staticmethod
    def _process_em(element: Element):
        def _merge_text(first: Optional[str], second: Optional[str]):
            if first:
                return first + second if second else first
            return second

        def _get_text_from_children(element: Element) -> str:
            v = None
            for x in element:
                v = _merge_text(v, x.text)
                v = _merge_text(v, _get_text_from_children(x))
                v = _merge_text(v, x.tail)
            return v

        # Get the text of all children and merge it
        element.text = _merge_text(element.text, _get_text_from_children(element))

        # Remove all children
        for x in element:
            element.remove(x)

    @staticmethod
    def _is_em(element: Element):
        return element.tag == "b" or element.tag == "i"

    @staticmethod
    def _apply_to_entity(element, func):
        if element.text is not None:
            element.text = func(element.text)
        if element.tail is not None:
            element.tail = func(element.tail)

    def run(self, e):
        if e.tag == "code" or e.tag == "pre":
            _HTMLTreeprocessor._apply_to_entity(e, _HTMLTreeprocessor._unescape)
        else:
            _HTMLTreeprocessor._apply_to_entity(e, html.escape)

        if _HTMLTreeprocessor._is_em(e):
            _HTMLTreeprocessor._process_em(e)
        else:
            for x in e:
                self.run(x)


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
        """Remove <code> tags nested inside <pre> tags, then replace the remaining <code> tags with <tt>."""
        _pre_code = r"<pre>(.|\n)*?<code>(.|\n)*?<\/code>(.|\n)*?<\/pre>"

        def replace_pre_code(match):
            text = match.string[match.start() : match.end()]
            text = text.replace("<code>", "")
            text = text.replace("</code>", "")
            return text

        text = re.sub(_pre_code, replace_pre_code, text)
        text = re.sub("<code>", "<tt>", text)
        text = re.sub("</code>", "</tt>", text)

        return text


class _RemovePreTagsPostprocessor(Postprocessor):
    """Replace <pre> tags with <tt>."""

    def run(self, text):
        text = re.sub("<pre>", "<tt>", text)
        text = re.sub("</pre>", "</tt>", text)

        return text


class _RemoveParagraphsPostprocessor(Postprocessor):
    def run(self, text):
        text = re.sub(r"<p>", "", text)
        text = re.sub(r"</p>", "", text)

        return text


class _RemoveHorizontalRulesPostprocessor(Postprocessor):
    def run(self, text):
        text = re.sub(r"<hr.*>", "", text)

        return text


class _RemoveBlockquotesPostprocessor(Postprocessor):
    def run(self, text):
        text = re.sub(r"<blockquote>", "", text)
        text = re.sub(r"</blockquote>", "", text)

        return text


class _BaseKnExtension(Extension):
    """Base Markdown extension class that registers and deregisters common components."""

    def extendMarkdown(self, _md) -> None:
        _md.preprocessors.deregister("html_block")

        # Remove not supported extensions
        _md.inlinePatterns.deregister("image_reference")
        _md.inlinePatterns.deregister("image_link")
        _md.inlinePatterns.deregister("short_image_ref")
        _md.inlinePatterns.deregister("autolink")
        _md.inlinePatterns.deregister("automail")
        _md.inlinePatterns.deregister("html")
        # _md.treeprocessors.deregister("unescape")

        _md.inlinePatterns.register(_KnimeProcessorAsterisk(r"\*"), "i_strong", 110)
        _md.inlinePatterns.register(_KnimeProcessorUnderscore(r"_"), "strong_i", 100)

        _md.postprocessors.register(
            _RemoveBlockquotesPostprocessor(), "knime_post_remove_blockquote", 50
        )
        _md.postprocessors.register(_KnimePostCode(), "knime_post_code", 10)

        _md.treeprocessors.register(_HTMLTreeprocessor(), "knime_code", 0)


class _KnExtension(_BaseKnExtension):
    """Basic extension for Knime schema."""

    def extendMarkdown(self, _md) -> None:
        super().extendMarkdown(_md)

        # Preprocessors
        _md.preprocessors.register(_HeadingPreprocessor(), "headings", 100)

        _md.postprocessors.register(_KnimeTable(), "knime_table", 200)
        _md.postprocessors.register(_KnimePostHeader(), "knime_post_headder", 200)


class _KnExtensionForOptions(_BaseKnExtension):
    """
    Markdown extension for option (parameter/setting) descriptions.

    Differs from _KnExtension by removing headings.
    """

    def extendMarkdown(self, _md) -> None:
        super().extendMarkdown(_md)

        # Preprocessors
        _md.preprocessors.register(
            _RemoveHeadingsPreprocessor(), "knime_pre_remove_headings", 100
        )

        # Postprocessors
        _md.postprocessors.register(
            _RemoveHorizontalRulesPostprocessor(),
            "knime_post_remove_horizontal_rule",
            60,
        )
        _md.postprocessors.register(_KnimeTable(), "knime_table", 200)
        _md.postprocessors.register(_KnimePostHeader(), "knime_post_headder", 200)


class _KnExtensionForTabs(_BaseKnExtension):
    """
    Markdown extension for tab descriptions (top-level parameter groups).

    Syntax that is not allowed in the final HTML:
    - headings
    - horizontal rules
    - pre blocks
    - blockquotes
    - p tags
    - tables
    """

    def extendMarkdown(self, _md) -> None:
        super().extendMarkdown(_md)

        # Preprocessors
        _md.preprocessors.register(
            _RemoveHeadingsPreprocessor(), "knime_pre_remove_headings", 100
        )

        # Postprocessors
        _md.postprocessors.register(
            _RemoveParagraphsPostprocessor(), "knime_post_remove_paragraph", 50
        )
        _md.postprocessors.register(
            _RemoveHorizontalRulesPostprocessor(),
            "knime_post_remove_horizontal_rule",
            60,
        )
        _md.postprocessors.register(
            _RemovePreTagsPostprocessor(), "knime_post_remove_pre_tags", 10
        )


class _KnExtensionForPorts(_BaseKnExtension):
    """
    Markdown extension for port descriptions.
    """

    def extendMarkdown(self, _md) -> None:
        super().extendMarkdown(_md)

        # Preprocessors
        _md.preprocessors.register(
            _RemoveHeadingsPreprocessor(), "knime_pre_remove_headings", 100
        )

        # Postprocessors
        _md.postprocessors.register(_KnimeTable(), "knime_table", 200)
        _md.postprocessors.register(_KnimePostHeader(), "knime_post_headder", 200)


class KnimeMarkdownParser:
    """ """

    def __init__(self):
        self.md = markdown.Markdown(
            extensions=[_KnExtension(), "sane_lists", "fenced_code", "tables"],
            output_format="xhtml",
        )

        # TODO headerlines
        self.md_basic = markdown.Markdown(
            extensions=[_KnExtension(), "sane_lists", "fenced_code"],
            output_format="xhtml",
        )

        self.md_options = markdown.Markdown(
            extensions=[_KnExtensionForOptions(), "sane_lists", "fenced_code"],
            output_format="xhtml",
        )

        self.md_tabs = markdown.Markdown(
            extensions=[_KnExtensionForTabs(), "sane_lists", "fenced_code"],
            output_format="xhtml",
        )

        self.md_ports = markdown.Markdown(
            extensions=[_KnExtensionForPorts(), "sane_lists", "fenced_code"],
            output_format="xhtml",
        )

    def parse_full_description(self, doc):
        doc = self._dedent(doc)
        return self.md.convert(doc)

    def parse_basic(self, doc):
        try:
            assert doc
            doc = self._dedent(doc)
            return self.md_basic.convert(doc)
        except AssertionError:
            return "<i>No description available.</i>"

    def parse_option_description(self, doc):
        if doc:
            doc = self._dedent(doc)
            return self.md_options.convert(doc)
        else:
            return "<i>No description available.</i>"

    def parse_port_description(self, doc):
        if doc:
            doc = self._dedent(doc)
            return self.md_ports.convert(doc)
        else:
            return "<i>No description available.</i>"

    def parse_tab_description(self, doc):
        if doc:
            doc = self._dedent(doc)
            return self.md_tabs.convert(doc)
        else:
            return ""

    def parse_ports(self, ports):
        return [
            {
                "name": port.name,
                "description": self.parse_port_description(port.description),
            }
            for port in ports
        ]

    def parse_options(self, options):
        return [
            {
                "name": option["name"],
                "description": self.parse_option_description(option["description"]),
            }
            for option in options
        ]

    def parse_tabs(self, tabs):
        return [
            {
                "name": tab["name"],
                "description": self.parse_tab_description(tab["description"]),
                "options": self.parse_options(tab["options"]),
            }
            for tab in tabs
        ]

    def _dedent(self, docstring):
        """Taken from PEP 257.

        An alternative to textwrap.dedent() that dedents the docstring such that
        it doesn't matter whether there is an empty line after the initial triple quotes.
        This prevents wrongful indentation resulting in <pre> blocks.
        """
        # Convert tabs to spaces (following the normal Python rules)
        # and split into a list of lines:
        lines = docstring.expandtabs().splitlines()

        # Determine minimum indentation (first line doesn't count):
        indent = sys.maxsize
        for line in lines[1:]:
            stripped = line.lstrip()
            if stripped:
                indent = min(indent, len(line) - len(stripped))

        # Remove indentation (first line is special):
        trimmed = [lines[0].strip()]
        if indent < sys.maxsize:
            for line in lines[1:]:
                trimmed.append(line[indent:].rstrip())

        # Strip off trailing and leading blank lines:
        while trimmed and not trimmed[-1]:
            trimmed.pop()
        while trimmed and not trimmed[0]:
            trimmed.pop(0)

        return "\n".join(trimmed)
