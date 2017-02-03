import pandas
import tempfile
import os


_types_ = None


def init(types):
    global _types_
    _types_ = types


def column_names_from_bytes(data_bytes):
    path = data_bytes.decode('utf-8')
    in_file = open(path, 'r')
    data_frame = pandas.read_csv(in_file, index_col=0, nrows=0, skiprows=1, na_values=['MissingCell'])
    in_file.close()
    return data_frame.columns.tolist()


def bytes_into_table(table, data_bytes):
    path = data_bytes.decode('utf-8')
    in_file = open(path, 'r')
    # TODO use types line to set dtype={'column1':numpy.int32}
    data_frame = pandas.read_csv(in_file, index_col=0, skiprows=1, na_values=['MissingCell'])
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
