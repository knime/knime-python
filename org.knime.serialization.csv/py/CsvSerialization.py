import pandas
import tempfile
import os
import numpy
import ast


_types_ = None
_array_type_ = None


def init(types):
    global _types_, _array_type_
    _types_ = types
    _array_type_ = {_types_.BOOLEAN_LIST.value, _types_.BOOLEAN_SET.value, _types_.INTEGER_LIST.value,
                    _types_.INTEGER_SET.value, _types_.LONG_LIST.value, _types_.LONG_SET.value,
                    _types_.DOUBLE_LIST.value, _types_.DOUBLE_SET.value, _types_.STRING_LIST.value,
                    _types_.STRING_SET.value, _types_.BYTES_LIST.value, _types_.BYTES_SET.value}


def column_names_from_bytes(data_bytes):
    path = data_bytes.decode('utf-8')
    in_file = open(path, 'r')
    data_frame = pandas.read_csv(in_file, index_col=0, nrows=0, skiprows=1)
    in_file.close()
    return data_frame.columns.tolist()


def bytes_into_table(table, data_bytes):
    path = data_bytes.decode('utf-8')
    in_file = open(path, 'r')
    types = in_file.readline().strip()[2:].split(',')
    names = pandas.read_csv(in_file, index_col=0, nrows=0).columns.tolist()
    in_file.seek(0)
    dtypes = {}
    for i in range(len(names)):
        name = names[i]
        col_type_id = int(types[i])
        if col_type_id == _types_.BOOLEAN.value:
            col_type = numpy.bool
        elif col_type_id == _types_.INTEGER.value:
            col_type = numpy.int32
        elif col_type_id == _types_.LONG.value:
            col_type = numpy.int64
        elif col_type_id == _types_.DOUBLE.value:
            col_type = numpy.float64
        else:
            col_type = numpy.str
        dtypes[name] = col_type
    data_frame = pandas.read_csv(in_file, index_col=0, skiprows=1, na_values=['MissingCell'], dtype=dtypes)
    for i in range(len(types)):
        col_type_id = int(types[i])
        if col_type_id in _array_type_:
            print('=== Converting column: ' + names[i])
            for j in range(len(data_frame)):
                index = data_frame.index[j]
                data_frame.set_value(index, names[i], ast.literal_eval(data_frame[names[i]][index]))
    table._data_frame = data_frame
    in_file.close()
    os.remove(path)


def table_to_bytes(table):
    path = tempfile.mkstemp(suffix='.csv', prefix='python-to-java-', text=True)[1]
    out_file = open(path, 'w')
    types_line = '#'
    for i in range(table.get_number_columns()):
        types_line += ',' + str(table.get_type(i).value)
    out_file.write(types_line + '\n')
    table._data_frame.to_csv(out_file, na_rep='MissingCell')
    out_file.close()
    return bytearray(path, 'utf-8')
