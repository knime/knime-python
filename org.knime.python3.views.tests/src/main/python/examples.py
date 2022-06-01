"""Examples using the knime_views package.

Run this script and open the html files that were created in the working directory
using your browser.
"""
import os

from numpy import save
import pythonpath  # adds knime_views to the Python path
import urllib.request

import knime_views as kv

import matplotlib.pyplot as plt
import seaborn as sns


def save_view(v, name):
    with open(f"{name}.html", "w", encoding="utf-8") as f:
        f.write(v.html)


def html_example():
    print("Generating view from HTML...")
    html = """
    <!DOCTYPE html>
    <html>
        <body><h1>Hello World</h1></body>
    </html>
    """
    html_view = kv.view(html)
    save_view(html_view, "html_view")


def svg_example():
    print("Generating view from SVG...")
    svg = """<svg version="1.1"
        width="300" height="200"
        xmlns="http://www.w3.org/2000/svg">

        <rect width="100%" height="100%" fill="red" />
        <circle cx="150" cy="100" r="80" fill="green" />
        <text x="150" y="125" font-size="60" text-anchor="middle" fill="white">SVG</text>
    </svg>"""
    svg_view = kv.view(svg)
    save_view(svg_view, "svg_view")


def png_example():
    print("Generating view from PNG...")
    png = urllib.request.urlopen("https://samples.fiji.sc/blobs.png").read()
    png_view = kv.view(png)
    save_view(png_view, "png_view")


def jpeg_example():
    print("Generating view from JPEG...")
    jpeg = urllib.request.urlopen("https://samples.fiji.sc/new-lenna.jpg").read()
    jpeg_view = kv.view(jpeg)
    save_view(jpeg_view, "jpeg_view")


def _plot_matplotlib():
    # https://matplotlib.org/stable/gallery/lines_bars_and_markers/bar_stacked.html#sphx-glr-gallery-lines-bars-and-markers-bar-stacked-py
    labels = ["G1", "G2", "G3", "G4", "G5"]
    men_means = [20, 35, 30, 35, 27]
    women_means = [25, 32, 34, 20, 25]
    men_std = [2, 3, 4, 1, 2]
    women_std = [3, 5, 2, 3, 3]
    width = 0.35  # the width of the bars: can also be len(x) sequence

    fig, ax = plt.subplots()

    ax.bar(labels, men_means, width, yerr=men_std, label="Men")
    ax.bar(labels, women_means, width, yerr=women_std, bottom=men_means, label="Women")

    ax.set_ylabel("Scores")
    ax.set_title("Scores by group and gender")
    ax.legend()

    return fig

def matplotlib_example():
    print("Generating view from matplotlib figure...")
    fig = _plot_matplotlib()
    mpl_view = kv.view(fig)
    save_view(mpl_view, "mpl_view")

def matplotlib_svg_example():
    print("Generating SVG view from matplotlib figure...")
    fig = _plot_matplotlib()
    mpl_view = kv.view_matplotlib(fig, format="svg")
    save_view(mpl_view, "mpl_view_svg")


def seaborn_example():
    print("Generating view from seaborn plot...")
    # https://seaborn.pydata.org/examples/faceted_lineplot.html
    sns.set_theme(style="ticks")
    dots = sns.load_dataset("dots")
    palette = sns.color_palette("rocket_r")
    sns.relplot(
        data=dots,
        x="time",
        y="firing_rate",
        hue="coherence",
        size="choice",
        col="align",
        kind="line",
        size_order=["T1", "T2"],
        palette=palette,
        height=5,
        aspect=0.75,
        facet_kws=dict(sharex=False),
    )
    seaborn_view = kv.view_matplotlib()
    save_view(seaborn_view, "seaborn_view")


if __name__ == "__main__":
    html_example()
    svg_example()
    png_example()
    jpeg_example()
    matplotlib_example()
    matplotlib_svg_example()
    seaborn_example()
