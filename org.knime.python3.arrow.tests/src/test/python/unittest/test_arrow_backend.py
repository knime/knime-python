import os
import tempfile
import contextlib
import unittest
import pyarrow as pa

import knime._arrow._backend as kab


class DummyJavaDataSink:
    def __init__(self, file_path):
        self.file_path = file_path
        self.num_batches = 0

    def getAbsolutePath(self):
        return self.file_path

    def setColumnarSchema(self, columnarSchema):
        self.columnarSchema = columnarSchema

    def reportBatchWritten(self, offset):
        self.num_batches += 1

    def setFinalSize(self, size):
        self.finalSize = size


@contextlib.contextmanager
def _sink_with_tmp_file():
    file_path = os.path.join(tempfile.gettempdir(), "test_file.arrow")
    java_data_sink = DummyJavaDataSink(file_path)
    data_sink = kab.ArrowDataSink(java_data_sink)
    try:
        yield data_sink, java_data_sink
    finally:
        data_sink.close()
        os.remove(file_path)


def _create_rb(num_rows):
    return pa.record_batch(
        data=[pa.array(list(range(num_rows)))],
        names=["data"],
    )


class ArrowDataSinkTest(unittest.TestCase):
    def setUp(self):
        # convert_schema requires the gateway. Therefore we mock it
        self._backup_convert_schema = kab.convert_schema
        kab.convert_schema = lambda schema: schema

    def tearDown(self) -> None:
        kab.convert_schema = self._backup_convert_schema

    def test_writing_single_batch(self):
        rb = _create_rb(10)
        with _sink_with_tmp_file() as (sink, java_data_sink):
            sink.write(rb)

        self.assertEqual(java_data_sink.num_batches, 1)
        self.assertEqual(java_data_sink.finalSize, 10)
        self.assertEqual(java_data_sink.columnarSchema, rb.schema)

    def test_writing_batches(self):
        rb1 = _create_rb(10)
        rb2 = _create_rb(10)
        rb3 = _create_rb(7)
        with _sink_with_tmp_file() as (sink, java_data_sink):
            sink.write(rb1)
            sink.write(rb2)
            sink.write(rb3)

        self.assertEqual(java_data_sink.num_batches, 3)
        self.assertEqual(java_data_sink.finalSize, 27)
        self.assertEqual(java_data_sink.columnarSchema, rb1.schema)

    def test_writing_table(self):
        single_batch_table = pa.Table.from_batches([_create_rb(10)])
        with _sink_with_tmp_file() as (sink, java_data_sink):
            sink.write(single_batch_table)

        self.assertEqual(java_data_sink.num_batches, 1)
        self.assertEqual(java_data_sink.finalSize, 10)
        self.assertEqual(java_data_sink.columnarSchema, single_batch_table.schema)

        multi_batch_table = pa.Table.from_batches(
            [_create_rb(10), _create_rb(10), _create_rb(8)]
        )
        with _sink_with_tmp_file() as (sink, java_data_sink):
            sink.write(multi_batch_table)

        self.assertEqual(java_data_sink.num_batches, 3)
        self.assertEqual(java_data_sink.finalSize, 28)
        self.assertEqual(java_data_sink.columnarSchema, multi_batch_table.schema)

    def test_writing_empty_table(self):
        empty_table = pa.Table.from_batches([_create_rb(0)])
        with _sink_with_tmp_file() as (sink, java_data_sink):
            sink.write(empty_table)

        # NB: Should report no batches
        self.assertEqual(java_data_sink.num_batches, 0)
        self.assertEqual(java_data_sink.finalSize, 0)
        self.assertEqual(java_data_sink.columnarSchema, empty_table.schema)

    def test_check_constant_batch_size(self):
        # Bigger batch in between
        with _sink_with_tmp_file() as (sink, _):
            sink.write(_create_rb(10))
            sink.write(_create_rb(10))
            self.assertRaises(ValueError, lambda: sink.write(_create_rb(11)))

        # Smaller batches are only allowed if they are the last batch
        with _sink_with_tmp_file() as (sink, _):
            sink.write(_create_rb(10))
            sink.write(_create_rb(10))
            sink.write(_create_rb(7))
            self.assertRaises(ValueError, lambda: sink.write(_create_rb(10)))


if __name__ == "__main__":
    unittest.main()
