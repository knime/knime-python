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

import datetime

import pyarrow as pa
import pyarrow.compute as pc

import knime


class EntryPoint(knime.client.EntryPoint):

    def testSimpleComputation(self, data_provider, data_callback):
        data = knime.data.mapDataProvider(data_provider)
        writer = knime.data.mapDataCallback(data_callback)
        # writer = knime_arrow_data.DataWriter(data_callback, client_server)

        def compute_fn(batch):
            column_str = batch.column(0)
            column_int = batch.column(1)

            column_0 = column_str  # Leave the string column
            column_1 = pc.add(column_int, column_int)  # Sum
            column_2 = pc.multiply(column_int, column_int)  # Product
            return pa.record_batch([column_0, column_1, column_2], ['0', 'sum', 'product'])

        # Loop over the data and apply the compute function
        for i in range(len(data)):
            input_batch = data[i]
            output_batch = compute_fn(input_batch)
            writer.write(output_batch)

        data.close()
        writer.close()

    def testLocalDate(self, data_provider):
        class LocalDateType(pa.ExtensionType):

            def __init__(self) -> None:
                pa.ExtensionType.__init__(
                    self, pa.int64(), "org.knime.localdate")

            def __arrow_ext_serialize__(self):
                # No parameters -> no metadata needed
                return b''

            @classmethod
            def __arrow_ext_deserialize__(self, storage_type, serialized):
                # No metadata
                return LocalDateType()

            def __arrow_ext_class__(self):
                return LocalDateArray

        class LocalDateArray(pa.ExtensionArray):
            def to_array(self):
                # TODO what about day-light savings and leap seconds?
                return [datetime.datetime.utcfromtimestamp(d * 24 * 60 * 60) for d in self.storage.to_numpy()]

        pa.register_extension_type(LocalDateType())
        data = knime.data.mapDataProvider(data_provider)

        # TODO There are multiple options to handle special types from Java
        #
        # * Not using Extension types:
        #   Only map them in the Data class when accessing higher level API (like pandas).
        #   We need to know the ColumnSchema to do this.
        #
        # * Using Extension types only in Python:
        #   Only map them in the Data class after reading.
        #   We need to know the ColumnSchema to do this.
        #
        # * Using Extension types in Python and Java:
        #   Save them using an Extension type in Java and let Arrow take care of reading it
        #   to the ExtenionArray.
        #   We do not need to know the ColumnSchema.
        #   Does not work for more special types (not our primitive types)

        print("Schema:", data.schema)
        batch0 = data[0]
        column0 = batch0.column(0)
        column0_datetime = pa.ExtensionArray.from_storage(
            LocalDateType(), column0)
        print("First batch:", batch0)
        print("First column:", column0)
        print("First column DateTime:", column0_datetime.to_array())

    def testZonedDateTime(self, data_provider):
        data = knime.data.mapDataProvider(data_provider)
        batch0 = data[0]
        column0 = batch0.column(0)
        print(column0)
        # TODO check the values

    def testStruct(self, data_provider):
        data = knime.data.mapDataProvider(data_provider)

        print("Schema:", data.schema)
        batch0 = data[0]
        print("First batch:", batch0)
        column0 = batch0.column(0)
        print("First column:", column0)
        # TODO check the values

    def testMultipleInputsOutputs(self, data_providers, data_callbacks):
        print("Called testMultipleInputsOutputs", data_providers)
        inputs = knime.data.mapDataProviders(data_providers)
        print("Inputs", inputs)
        # TODO check the values
        # TODO test multiple outputs

    class Java:
        implements = [
            "org.knime.python3.arrow.TestUtils.ArrowTestEntryPoint"]


knime.client.connectToJava(EntryPoint())
