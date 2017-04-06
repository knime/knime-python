_format = '%Y-%m-%d %H:%M:%S.%f'


def serialize(object_value):
    return object_value.strftime(_format)[:-3].encode('utf-8')
