from datetime import datetime

_format = '%H:%M:%S.%f'

def deserialize(data_bytes):
    return datetime.strptime(data_bytes.decode('utf-8') + '000', _format).time()
