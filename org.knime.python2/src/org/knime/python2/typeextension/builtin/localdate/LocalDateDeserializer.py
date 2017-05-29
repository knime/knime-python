from datetime import datetime

_format = '%Y-%m-%d'


def deserialize(data_bytes):
    return datetime.strptime(data_bytes.decode('utf-8'), _format).date()
