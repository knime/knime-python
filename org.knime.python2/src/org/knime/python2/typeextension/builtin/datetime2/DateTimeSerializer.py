try:
    from pandas.tslib import Timestamp
    _tslib_available = True
except ImportError:
    _tslib_available = False

def serialize(object_value):
    if _tslib_available and type(object_value) is Timestamp:
        object_value = object_value.to_pydatetime()
    #format: yyyy-mm-dd HH:MM:SS (+microseconds and offset to UTC if available) 
    datestr = object_value.isoformat(' ')
    #Add timezone name (offset already included in isoformat)
    if object_value.tzname():
        datestr += '[' + object_value.tzname() + ']'
    #Find +/- sign of timezone offset
    tzidx = max(datestr.find('+'), datestr.find('-', 10))
    if tzidx < 0:
        tzidx = len(datestr)
    #If microseconds available cut down to millis otherwise add millis
    if object_value.microsecond > 0:
        datestr = datestr[:tzidx-3] + datestr[tzidx:]
    else:
        datestr = datestr[:tzidx] + '.000' + datestr[tzidx:]
    return datestr.encode('utf-8')
