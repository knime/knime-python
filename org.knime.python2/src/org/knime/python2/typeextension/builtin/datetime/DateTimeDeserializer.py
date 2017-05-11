from datetime import datetime

_format = '%Y-%m-%d %H:%M:%S.%f'


def deserialize(data_bytes):
    with open('/home/clemens/pythonkernellog.txt', 'a') as writer:
        writer.write('Date string: ' + data_bytes.decode('utf-8') + '\n')
    return datetime.strptime(data_bytes.decode('utf-8') + '000', _format)
