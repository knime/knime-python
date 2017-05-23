def serialize(object_value):
    #format: yyyy-mm-dd
    datestr = object_value.isoformat()[:10]
    return datestr.encode('utf-8')
