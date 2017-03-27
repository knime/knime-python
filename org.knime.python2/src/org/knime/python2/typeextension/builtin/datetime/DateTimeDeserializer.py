from datetime import datetime

_format = '%Y-%m-%d %H:%M:%S.%f'


def deserialize(data_bytes):
    return datetime.strptime(data_bytes.decode('utf-8') + '000', _format)
