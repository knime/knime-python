_format = '%Y-%m-%d'


def serialize(object_value):
    datestr = object_value.strftime(_format)
    #Note: The datetime API only handles dates starting from year 1, no need to worry about negative years (clemens.vonschwerin@knime.com)
    if object_value.year < 10:
        datestr = '000' + datestr
    elif object_value.year < 100:
        datestr = '00' + datestr
    elif object_value.year < 1000:
        datestr = '0' + datestr
    return datestr.encode('utf-8')
