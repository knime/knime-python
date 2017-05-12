def serialize(object_value):
    #format: yyyy-mm-dd HH:MM:SS (microseconds only if available)
    datestr = object_value.isoformat(' ')
    if object_value.microsecond > 0:
        datestr = datestr[:-3]
    else:
        datestr = datestr + '.000'
    return datestr.encode('utf-8')
