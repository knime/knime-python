_format = '%Y-%m-%d %H:%M:%S.%f'


def serialize(object_value):
    return bytearray(object_value.strftime(_format)[:-3], 'utf-8')
