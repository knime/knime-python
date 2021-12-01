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

from typing import Type, Union

from pandas.core.dtypes.dtypes import register_extension_dtype
import pandas.api.extensions as pdext

import pyarrow as pa
import knime_gateway as kg
import knime_types as kt
import knime_arrow_types as kat
import knime_arrow_struct_dict_encoding as kas
import pandas as pd
import numpy as np


def pandas_df_to_arrow(
    data_frame: pd.DataFrame, to_batch=False
) -> Union[pa.Table, pa.RecordBatch]:
    if data_frame.shape == (
        0,
        0,
    ):
        if to_batch:
            return pa.RecordBatch.from_arrays([])
        else:
            return pa.table([])

    # TODO pandas column names must not be strings. This causes errors
    # Convert the index to a str series and prepend to the data_frame

    # extract and drop index from DF
    row_keys = data_frame.index.to_series().astype(str)
    row_keys.name = "<Row Key>"  # TODO what is the right string?
    data_frame = pd.concat(
        [row_keys.reset_index(drop=True), data_frame.reset_index(drop=True)],
        axis=1,
    )

    if to_batch:
        return pa.RecordBatch.from_pandas(data_frame)
    else:
        return pa.Table.from_pandas(data_frame)


def arrow_data_to_pandas_df(data: Union[pa.Table, pa.RecordBatch]) -> pd.DataFrame:
    data_frame = data.to_pandas()
    # The first column is interpreted as the index (row keys)
    data_frame.set_index(data_frame.columns[0], inplace=True)
    return data_frame


@register_extension_dtype
class PandasLogicalTypeExtensionType(pdext.ExtensionDtype):
    def __init__(self, storage_type: pa.DataType, logical_type: str, converter):
        self._storage_type = storage_type
        self._logical_type = logical_type
        self._converter = converter

        # used by pandas to query all attributes of this ExtensionType
        self._metadata = ("_storage_type", "_logical_type", "_converter")

    na_value = None
    type = bytes  # We just say that this is raw data?! No need to be interpreted :)

    @property
    def name(self):
        return f"PandasLogicalTypeExtensionType({self._storage_type}, {self._logical_type})"

    def construct_array_type(self):
        return KnimePandasExensionArray

    def construct_from_string(cls: Type[pdext.ExtensionDtype], string: str):
        # TODO implement this?
        raise NotImplementedError("construct from string not available yet")

    def __from_arrow__(self, arrow_array):
        return KnimePandasExensionArray(
            self._storage_type, self._logical_type, self._converter, arrow_array
        )

    def __str__(self):
        return f"PandasLogicalTypeExtensionType({self._storage_type}, {self._logical_type})"


class KnimePandasExensionArray(pdext.ExtensionArray):
    def __init__(
        self,
        storage_type: pa.DataType,
        logical_type: str,
        converter,
        data: Union[pa.Array, pa.ChunkedArray],
    ):
        if data is None:
            raise ValueError("Cannot create empty KnimePandasExtensionArray")
        self._data = data
        self._converter = converter
        self._storage_type = storage_type
        self._logical_type = logical_type

    def __arrow_array__(self, type=None):
        return self._data

    @classmethod
    def _from_sequence(
        cls,
        scalars,
        dtype=None,
        copy=None,
        storage_type=None,
        logical_type=None,
        converter=None,
    ):
        if scalars is None:
            raise ValueError("Cannot create KnimePandasExtensionArray from empty data")

        # easy case
        if isinstance(scalars, pa.Array) or isinstance(scalars, pa.ChunkedArray):
            if not isinstance(scalars.type, kat.LogicalTypeExtensionType):
                raise ValueError(
                    "KnimePandasExtensionArray must be backed by LogicalTypeExtensionType values"
                )
            return KnimePandasExensionArray(
                scalars.type.storage_type,
                scalars.type.logical_type,
                scalars.type._converter,
                scalars,
            )

        # needed for pandas ExtensionArray API
        arrow_type = kat.LogicalTypeExtensionType(converter, storage_type, logical_type)

        if storage_type is None:
            raise ValueError(
                "Can only create KnimePandasExtensionArray from a sequence if the storage type is given."
            )

        a = pa.array(scalars, type=storage_type)
        extension_array = pa.ExtensionArray.from_storage(arrow_type, a)
        return KnimePandasExensionArray(
            storage_type, logical_type, converter, extension_array
        )

    @classmethod
    def _from_factorized(cls, values, original):
        # needed for pandas ExtensionArray API
        raise NotImplementedError(
            "KnimePandasExtensionArray cannot be created from factorized yet."
        )

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._data[item].as_py()
        elif isinstance(item, slice):
            (start, stop, step) = item.indices(len(self._data))
            # if step == 1:
            #     return self._data.slice(offset=start, length=stop - start)

            indices = list(range(start, stop, step))
            return self.take(indices)
        elif isinstance(item, list):
            # fetch objects at the individual indices
            return self.take(item)
        elif isinstance(item, np.ndarray):
            # masked array
            raise NotImplementedError("Cannot index using masked array from numpy yet.")

    def __setitem__(self, item, value):
        raise NotImplementedError(
            "Columns with non-primitive KNIME data types do not support modification yet."
        )

    def __len__(self):
        return len(self._data)

    def __eq__(self, other) -> bool:
        if not isinstance(other, KnimePandasExensionArray):
            return False
        return (
            other._storage_type == self._storage_type
            and other._logical_type == self._logical_type
            and other._converter == self._converter
            and other._data == self._data
        )

    @property
    def dtype(self):
        # needed for pandas ExtensionArray API
        return PandasLogicalTypeExtensionType(
            self._storage_type, self._logical_type, self._converter
        )

    @property
    def nbytes(self):
        # needed for pandas ExtensionArray API
        return self._data.nbytes

    def isna(self):
        # needed for pandas ExtensionArray API
        return self._data.is_null().to_numpy()

    def take(self, indices, *args, **kwargs) -> "KnimePandasExensionArray":
        # needed for pandas ExtensionArray API
        # TODO: handle allow_fill and fill_value kwargs?

        storage = kat._to_storage_array(self._data)
        taken = storage.take(indices)
        wrapped = kat._to_extension_array(taken, self._data.type)

        return self._from_sequence(
            wrapped,
            storage_type=self._storage_type,
            logical_type=self._logical_type,
            converter=self._converter,
        )

    def copy(self):
        # needed for pandas ExtensionArray API
        # TODO: do we really want to copy the data? This thing is read only anyways... Unless we implement __setitem__ and concat
        return self

    @classmethod
    def _concat_same_type(cls, to_concat):
        # needed for pandas ExtensionArray API

        if len(to_concat) < 1:
            raise ValueError("Nothing to concatenate")
        elif len(to_concat) == 1:
            return to_concat[0]

        chunks = []
        for pandas_ext_array in to_concat:
            d = pandas_ext_array._data
            if isinstance(d, pa.ChunkedArray):
                chunks += d.chunks
            else:
                chunks.append(d)

        combined_data = pa.chunked_array(chunks)
        first = to_concat[0]
        return KnimePandasExensionArray(
            first._storage_type, first._logical_type, first._converter, combined_data
        )
