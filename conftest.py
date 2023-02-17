import os
import sys

# Utils


def join(*paths):
    return os.path.normpath(os.path.join(*paths))


def add_path(*rel_path):
    sys.path.insert(0, join(__file__, "..", *rel_path))


def add_src_main_path(plugin_name):
    add_path(plugin_name, "src", "main", "python")


# Ignore the knime_kernel_test.py because it is run from a JUnit test

collect_ignore = [
    join(
        "org.knime.python3.scripting.tests",
        "src",
        "test",
        "python",
        "knime_kernel_test.py",
    )
]

# Add paths to the python sources

add_src_main_path("org.knime.python3")
add_src_main_path("org.knime.python3.nodes")
add_src_main_path("org.knime.python3.scripting")
add_src_main_path("org.knime.python3.scripting.tests")
add_src_main_path("org.knime.python3.views")
add_src_main_path("org.knime.python3.arrow")
add_src_main_path("org.knime.python3.arrow.types")
