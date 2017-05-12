from datetime import datetime

_format = '%Y-%m-%d'


def deserialize(data_bytes):
    with open('/home/clemens/pythonkernellog.txt', 'a') as writer:
        writer.write('(LocalDateDeserializer) Date string: ' + data_bytes.decode('utf-8') + '\n')
    return datetime.strptime(data_bytes.decode('utf-8'), _format).date()
