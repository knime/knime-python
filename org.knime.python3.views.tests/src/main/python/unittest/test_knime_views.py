import os
import pythonpath  # adds knime_views to the Python path

import unittest
import knime_views as kv


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
            self.assertTrue('<img src="data:image/png;base64' in png_view.html)
            self.assertTrue(is_html(png_view.html))

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
            self.assertTrue('<img src="data:image/jpeg;base64' in jpeg_view.html)
            self.assertTrue(is_html(jpeg_view.html))

        for jpeg in self.jpegs:
            # Using kv.view
            check_view(kv.view(jpeg))

            # Using kv.view_html
            check_view(kv.view_jpeg(jpeg))

            # Using an object that has _repr_jpeg_
            check_view(kv.view(ViewableClass(jpeg=jpeg)))

    def test_repr_choice(self):
        # HTML before everything else
        html_viewable = ViewableClass(
            html=self.htmls[0], svg=self.svgs[0], png=self.pngs[0], jpeg=self.jpegs[0]
        )
        html_view = kv.view(html_viewable)
        self.assertEqual(html_view.html, self.htmls[0])

        # SVG next
        svg_viewable = ViewableClass(
            svg=self.svgs[0], png=self.pngs[0], jpeg=self.jpegs[0]
        )
        svg_view = kv.view(svg_viewable)
        self.assertTrue(self.svgs[0] in svg_view.html)

        # PNG next
        png_viewable = ViewableClass(png=self.pngs[0], jpeg=self.jpegs[0])
        png_view = kv.view(png_viewable)
        self.assertTrue('<img src="data:image/png;base64' in png_view.html)

        # JPEG next
        jpeg_viewable = ViewableClass(jpeg=self.jpegs[0])
        jpeg_view = kv.view(jpeg_viewable)
        self.assertTrue('<img src="data:image/jpeg;base64' in jpeg_view.html)

    def test_matplotlib_view(self):
        def check_view(matplotlib_view):
            self.assertIsInstance(matplotlib_view, kv.NodeView)
            self.assertTrue(is_html(matplotlib_view.html))
            self.assertTrue('<img src="data:image/png;base64' in matplotlib_view.html)

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

        import matplotlib.pyplot as plt
        import seaborn as sns

        data_x = list(range(10))
        data_y = [i + 2 for i in range(10)]

        # Stateful approach
        plt.plot(data_x, data_y)
        check_view(kv.view_matplotlib(format="svg"))


if __name__ == "__main__":
    unittest.main()
