from setuptools import setup

setup(
    name="knime_extension",
    version="5.1.0",
    description="API for KNIME Python extension development to facilitate autocompletion",
    url="https://www.knime.com",
    author="Steffen Fissler, KNIME GmbH, Konstanz, Germany",
    license="GPLv3",
    packages=["knime", "knime.api", "knime.extension", "knime_extension"],
    zip_safe=False,
)
