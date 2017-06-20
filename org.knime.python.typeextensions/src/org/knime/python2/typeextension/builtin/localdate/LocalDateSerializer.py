def serialize(object_value):
    #format: yyyy-mm-dd
    datestr = object_value.isoformat()
    return datestr.encode('utf-8')
