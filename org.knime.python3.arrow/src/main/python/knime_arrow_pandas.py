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

from typing import Union
import pyarrow as pa
import knime_gateway as kg
import knime_types as kt
import knime_arrow_types as kat
import pandas as pd
import numpy as np


def pandas_df_to_arrow(
    data_frame: pd.DataFrame, to_batch=False
) -> Union[pa.Table, pa.RecordBatch]:
    # TODO pandas column names must not be strings. This causes errors
    # Convert the index to a str series and prepend to the data_frame
    row_keys = data_frame.index.to_series().astype(str)
    row_keys.name = "<Row Key>"  # TODO what is the right string?
    data_frame = pd.concat(
        [row_keys.reset_index(drop=True), data_frame.reset_index(drop=True)], axis=1,
    )

    schema = _extract_schema(data_frame)
    # pyarrow doesn't allow to customize the conversion from pandas, so we convert the corresponding columns to storage
    # format in the pandas DataFrame
    storage_df, storage_schema = _to_storage_data_frame(data_frame, schema)
    if to_batch:
        arrow_data = pa.RecordBatch.from_pandas(storage_df, schema=storage_schema)
    else:
        arrow_data = pa.table(storage_df, schema=storage_schema)
    return kat.storage_to_extension(arrow_data, schema)


def _extract_schema(data_frame: pd.DataFrame):
    dtypes = data_frame.dtypes
    columns = []

    for (idx, (column, dtype)) in enumerate(dtypes.items()):
        try:
            column_type = _to_arrow_type(
                data_frame[column][0], is_row_key_column=(idx == 0)
            )
            columns.append((column, column_type,))
        except ValueError as e:
            raise ValueError(
                f"Could not understand the data type in column {idx}:{column}={data_frame[column][0]}"
            )
    return pa.schema(columns)


_pd_to_arrow_type_map = {
    np.int32: pa.int32(),
    int: pa.int32(),
    np.int64: pa.int64(),
    np.uint32: pa.uint32(),
    np.int8: pa.int8(),
    np.float32: pa.float32(),
    np.float64: pa.float64(),
    str: pa.string(),
    np.bool_: pa.bool_(),
    bool: pa.bool_(),
}


def _to_arrow_type(first_value, is_row_key_column: bool = False):
    # converter = kg.gateway().org.knime.core.data.v2.DefaultValueFactories
    # TODO: use logic from DefaultValueFactories in Java
    t = type(first_value)
    if is_row_key_column and t == str:
        return kat.LogicalTypeExtensionType(
            kt.FallbackPythonValueFactory(),
            pa.string(),
            '{"value_factory_class":"org.knime.core.data.v2.value.DefaultRowKeyValueFactory"}',
        )
    elif t == list or t == np.ndarray:
        inner = _to_arrow_type(first_value[0])
        return pa.list_(inner)
    elif t in _pd_to_arrow_type_map:
        return _pd_to_arrow_type_map[t]
    else:
        return kat.to_extension_type(first_value)


def _to_storage_data_frame(df: pd.DataFrame, schema: pa.Schema):
    storage_schema = []
    for name, arrow_type in zip(schema.names, schema.types):
        storage_series, storage_type = _series_to_storage(df[name], arrow_type)
        storage_schema.append((name, storage_type))
        df[name] = storage_series
    return df, pa.schema(storage_schema)


def _series_to_storage(series: pd.Series, arrow_type: pa.DataType):
    storage_type = kat.get_storage_type(arrow_type)
    if kat.needs_conversion(arrow_type):
        storage_fn = kat.get_object_to_storage_fn(arrow_type)
        storage_series = pd.Series([storage_fn(x) for x in series])
        return storage_series, storage_type
    else:
        return series, storage_type


def arrow_data_to_pandas_df(data: Union[pa.Table, pa.RecordBatch]) -> pd.DataFrame:
    logical_columns = [
        i
        for i, field in enumerate(data.schema)
        if kat.contains_knime_extension_type(field.type)
    ]
    storage_data = kat.to_storage_data(data)
    data_frame = storage_data.to_pandas()

    _encode_df(data_frame, logical_columns, data.schema)
    # The first column is interpreted as the index (row keys)
    data_frame.set_index(data_frame.columns[0], inplace=True)
    return data_frame


def _encode_df(df: pd.DataFrame, logical_columns, schema: pa.Schema):
    for i in logical_columns:
        field = schema.field(i)
        t = field.type
        if hasattr(t, "needs_conversion") and t.needs_conversion:
            df[field.name] = df[field.name].apply(t.decode)
