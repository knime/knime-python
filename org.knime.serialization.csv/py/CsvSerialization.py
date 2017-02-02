from pandas import DataFrame


_types_ = None


def init(types):
    global _types_
    _types_ = types


def column_names_from_bytes(data_bytes):
    # TODO read first line and parse
    return None


def bytes_into_table(table, data_bytes):
    # TODO table._data_frame = DataFrame.from_csv()
    pass


def table_to_bytes(table):
    # TODO table._data_frame.to_csv()
    return None
