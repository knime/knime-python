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
@author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
"""

DATA_PROVIDERS = {}
DATA_CALLBACKS = {}


def registerDataProvider(identifier: str, mapper):
    """Register a mapper which maps Java objects providing data to Python objects with an
        pythonic API.

    Args:
        identifier: The identifier of the data provider. Will be compared to the return value
            of `getIdentifier` on the Java object.
        mapper: A function taking the Java object as an argument and returning a Python object
            which provides the data in with a Pythonic API.
    """
    DATA_PROVIDERS[identifier] = mapper


def registerDataCallback(identifier: str, mapper):
    """Register a mapper which maps Java objects getting data from Python with an pythonic API.

    Args:
        identifier: The identifier of the data provider. Will be compared to the return value
            of `getIdentifier` on the Java object.
        mapper: A function taking the Java object as an argument and returning a Python object
            which accepts data with a Pythonic API.
    """
    DATA_CALLBACKS[identifier] = mapper


def mapDataProvider(data_provider):
    """Map a Java object which provides data to an Python object which gives access to the data
        using a Pythonic API.

    There must be a mapper registerd for the type of data provider that is given using
    `registerDataProvider(identifier, mapper)`.

    Args:
        data_provider: The Java object providing data.

    Raises:
        ValueError: If no mapper is registered for the type of data provider.
    """
    id = data_provider.getIdentifier()
    if id not in DATA_PROVIDERS:
        raise ValueError(
            f"No mapper registerd for identifier {id}. "
            "Are you missing a KNIME Extension?"
            "If this is your own extension make sure to register the extension "
            "using `knime.data.registerDataProvider(id, mapper)`")
    return DATA_PROVIDERS[id](data_provider)


def mapDataCallback(data_callback):
    """Map a Java object which collects data to a Python object with an Pythonic API.

    There must be a mapper registerd for the type of data provider that is given using
    `registerDataCallback(identifier, mapper)`.

    Args:
        data_callback: The Java object collecting data.

    Raises:
        ValueError: If no mapper is registered for the type of data callback.
    """
    id = data_callback.getIdentifier()
    if id not in DATA_CALLBACKS:
        raise ValueError(
            f"No mapper registerd for identifier {id}. "
            "Are you missing a KNIME Extension?"
            "If this is your own extension make sure to register the extension "
            "using `knime.data.registerDataCallback(id, mapper)`")
    return DATA_CALLBACKS[id](data_callback)


def mapDataProviders(data_providers):
    # TODO docstring
    return [mapDataProvider(d) for d in data_providers]


def mapDataCallbacks(data_callbacks):
    # TODO docstring
    return [mapDataCallback(d) for d in data_callbacks]
