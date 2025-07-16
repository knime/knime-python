import os
import base64

import unittest
import knime.api.views as kv


class ViewableClass:
    def __init__(self, html=None, svg=None, png=None, jpeg=None):
        if html is not None:
            self._repr_html_ = lambda: html

        if svg is not None:
            self._repr_svg_ = lambda: svg

        if png is not None:
            self._repr_png_ = lambda: png

        if jpeg is not None:
            self._repr_jpeg_ = lambda: jpeg


def is_html(s):
    return s.lstrip().startswith("<!DOCTYPE html>")


def has_reporting_code(s: str):
    return "setReportingContent(" in s


def open_test_file(name, mode):
    return open(os.path.normpath(os.path.join(__file__, "..", name)), mode)


class KnimeViewsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.htmls = [
            """<!DOCTYPE html>
                <html>
                    <body>Hello World</body>
                </html>
            """,
            """

                <!DoCTyPE hTml>
                Strange document body but marked as HTML.
            """,
        ]

        self.svgs = [
            """<svg version="1.1"
                width="300" height="200"
                xmlns="http://www.w3.org/2000/svg">

                <rect width="100%" height="100%" fill="red" />
                <circle cx="150" cy="100" r="80" fill="green" />
                <text x="150" y="125" font-size="60" text-anchor="middle" fill="white">SVG</text>

            </svg>"""
        ]

        with open_test_file("small.png", "rb") as f:
            self.pngs = [f.read()]

        with open_test_file("small.jpeg", "rb") as f:
            self.jpegs = [f.read()]

    def test_html_view(self):
        def check_view(html_view, html):
            self.assertIsInstance(html_view, kv.NodeView)
            self.assertEqual(html_view.html, html)
            with self.assertRaises(NotImplementedError):
                html_view.render()
            self.assertFalse(html_view.can_be_used_in_report)

        for html in self.htmls:
            # Using kv.view
            check_view(kv.view(html), html)

            # Using kv.view_html
            check_view(kv.view_html(html), html)

            # Using an object that has _repr_html_
            check_view(kv.view(ViewableClass(html=html)), html)

    def test_svg_view(self):
        def check_view(svg_view, svg):
            self.assertIsInstance(svg_view, kv.NodeView)
            self.assertTrue(svg in svg_view.html)
            self.assertTrue(is_html(svg_view.html))
            self.assertEqual(svg_view.render(), svg)
            self.assertTrue(svg_view.can_be_used_in_report)
            self.assertTrue(has_reporting_code(svg_view.html))

        for svg in self.svgs:
            # Using kv.view
            check_view(kv.view(svg), svg)

            # Using kv.view_svg
            check_view(kv.view_svg(svg), svg)

            # Using an object that has _repr_svg_
            check_view(kv.view(ViewableClass(svg=svg)), svg)

    def test_png_view(self):
        def check_view(png_view):
            self.assertIsInstance(png_view, kv.NodeView)
            self.assertTrue('<img id="view-container" src="data:image/png;base64' in png_view.html)
            self.assertTrue(is_html(png_view.html))
            self.assertEqual(png_view.render(), png)
            self.assertTrue(png_view.can_be_used_in_report)
            self.assertTrue(has_reporting_code(png_view.html))

        for png in self.pngs:
            # Using kv.view
            check_view(kv.view(png))

            # Using kv.view_png
            check_view(kv.view_png(png))

            # Using an object that has _repr_png_
            check_view(kv.view(ViewableClass(png=png)))

    def test_jpeg_view(self):
        def check_view(jpeg_view):
            self.assertIsInstance(jpeg_view, kv.NodeView)
            self.assertTrue('<img id="view-container" src="data:image/jpeg;base64' in jpeg_view.html)
            self.assertTrue(is_html(jpeg_view.html))
            self.assertEqual(jpeg_view.render(), jpeg)
            self.assertTrue(jpeg_view.can_be_used_in_report)
            self.assertTrue(has_reporting_code(jpeg_view.html))

        for jpeg in self.jpegs:
            # Using kv.view
            check_view(kv.view(jpeg))

            # Using kv.view_html
            check_view(kv.view_jpeg(jpeg))

            # Using an object that has _repr_jpeg_
            check_view(kv.view(ViewableClass(jpeg=jpeg)))

    def test_fail_on_non_supported_input(self):
        # string that is not HTML or SVG
        with self.assertRaises(ValueError) as cm:
            kv.view("foo bar")
        self.assertEqual(str(cm.exception), "no view could be created for foo bar")

        # bytes that is not PNG or JPEG
        with self.assertRaises(ValueError) as cm:
            kv.view(b"foo bar")
        self.assertEqual(str(cm.exception), "no view could be created for b'foo bar'")

        # obj without any _repr_*_ method in view_ipy_repr
        non_viewable_obj = ViewableClass()
        with self.assertRaises(ValueError) as cm:
            kv.view_ipy_repr(non_viewable_obj)
        self.assertEqual(
            str(cm.exception), "no _repr_*_ function is implemented by obj"
        )

    def test_repr_with_metadata(self):
        html_viewable = ViewableClass(html=(self.htmls[0], "some metadata"))
        html_view = kv.view(html_viewable)
        self.assertEqual(html_view.html, self.htmls[0])

    def test_repr_choice(self):
        # HTML before everything else
        html_viewable = ViewableClass(
            html=self.htmls[0], svg=self.svgs[0], png=self.pngs[0], jpeg=self.jpegs[0]
        )
        html_view = kv.view(html_viewable)
        self.assertEqual(html_view.html, self.htmls[0])
        self.assertEqual(html_view.render(), self.svgs[0])

        # SVG next
        svg_viewable = ViewableClass(
            svg=self.svgs[0], png=self.pngs[0], jpeg=self.jpegs[0]
        )
        svg_view = kv.view(svg_viewable)
        self.assertTrue(self.svgs[0] in svg_view.html)
        self.assertEqual(svg_view.render(), self.svgs[0])

        # PNG next
        png_viewable = ViewableClass(png=self.pngs[0], jpeg=self.jpegs[0])
        png_view = kv.view(png_viewable)
        self.assertTrue('<img id="view-container" src="data:image/png;base64' in png_view.html)
        self.assertEqual(png_view.render(), self.pngs[0])

        # JPEG next
        jpeg_viewable = ViewableClass(jpeg=self.jpegs[0])
        jpeg_view = kv.view(jpeg_viewable)
        self.assertTrue('<img id="view-container" src="data:image/jpeg;base64' in jpeg_view.html)
        self.assertEqual(jpeg_view.render(), self.jpegs[0])

    def test_repr_html_render(self):
        # HTML + SVG
        html_svg_viewable = ViewableClass(
            html=self.htmls[0], svg=self.svgs[0], png=self.pngs[0], jpeg=self.jpegs[0]
        )
        self.assertEqual(kv.view(html_svg_viewable).render(), self.svgs[0])

        # HTML + PNG
        html_png_viewable = ViewableClass(
            html=self.htmls[0], png=self.pngs[0], jpeg=self.jpegs[0]
        )
        self.assertEqual(kv.view(html_png_viewable).render(), self.pngs[0])

        # HTML + JPEG
        html_jpeg_viewable = ViewableClass(html=self.htmls[0], jpeg=self.jpegs[0])
        self.assertEqual(kv.view(html_jpeg_viewable).render(), self.jpegs[0])

        # HTML only -> not renderable
        html_only_viewable = ViewableClass(html=self.htmls[0])
        html_view = kv.view(html_only_viewable)
        with self.assertRaises(NotImplementedError):
            html_view.render()

    def test_matplotlib_view(self):
        def check_view(matplotlib_view):
            self.assertIsInstance(matplotlib_view, kv.NodeView)
            self.assertTrue(is_html(matplotlib_view.html))
            self.assertTrue('<img id="view-container" src="data:image/png;base64' in matplotlib_view.html)
            render_repr = matplotlib_view.render()
            self.assertTrue(
                base64.b64encode(render_repr).decode("ascii") in matplotlib_view.html
            )
            self.assertTrue(matplotlib_view.can_be_used_in_report)
            self.assertTrue(has_reporting_code(matplotlib_view.html))

        import matplotlib.pyplot as plt
        import seaborn as sns

        data_x = list(range(10))
        data_y = [i + 2 for i in range(10)]

        # Stateful approach
        plt.plot(data_x, data_y)
        check_view(kv.view_matplotlib())

        # object oriented approach
        fig, ax = plt.subplots()
        ax.plot(data_x, data_y)
        check_view(kv.view_matplotlib(fig))

        # object oriented approach + view()
        fig, ax = plt.subplots()
        ax.plot(data_x, data_y)
        check_view(kv.view(fig))

        # Seaborn stateful
        sns.lineplot(x="x", y="y", data={"x": data_x, "y": data_y})
        check_view(kv.view_matplotlib())

        # Seaborn stateful
        sns.lineplot(x="x", y="y", data={"x": data_x, "y": data_y})
        check_view(kv.view_seaborn())

        # Seaborn figure object
        lp = sns.lineplot(x="x", y="y", data={"x": data_x, "y": data_y})
        check_view(kv.view(lp.figure))

        # This shouldn't do anything
        plt.show()

    def test_matplotlib_view_svg(self):
        def check_view(matplotlib_view):
            self.assertIsInstance(matplotlib_view, kv.NodeView)
            self.assertTrue(is_html(matplotlib_view.html))
            self.assertTrue(
                'xmlns="http://www.w3.org/2000/svg"' in matplotlib_view.html
            )
            render_repr = matplotlib_view.render()
            self.assertTrue(render_repr in matplotlib_view.html)
            self.assertTrue(matplotlib_view.can_be_used_in_report)
            self.assertTrue(has_reporting_code(matplotlib_view.html))

        import matplotlib.pyplot as plt

        data_x = list(range(10))
        data_y = [i + 2 for i in range(10)]

        # Stateful approach
        plt.plot(data_x, data_y)
        check_view(kv.view_matplotlib(format="svg"))

    def test_matplotlib_fail_unsupported_format(self):
        import matplotlib.pyplot as plt

        data_x = list(range(10))
        data_y = [i + 2 for i in range(10)]

        # Stateful approach
        plt.plot(data_x, data_y)

        with self.assertRaises(ValueError) as cm:
            kv.view_matplotlib(format="my_crazy_format")
        self.assertEqual(
            str(cm.exception),
            'unsupported format for matplotlib view: my_crazy_format. Use "png" or "svg".',
        )


if __name__ == "__main__":
    unittest.main()
