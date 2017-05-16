from datetime import datetime
from dateutil import tz

_format = '%Y-%m-%d %H:%M:%S.%f'

#Deserializes LocalDateTime, ZonedDateTime and legacy DateTime
def deserialize(data_bytes):
    datestr = data_bytes.decode('utf-8')
    dt = datetime.strptime(datestr[:23] + '000', _format)
    #Timezone information available ?
    if len(datestr) > 23:
        #Calculate timezone offset
        sgn = 1
        if datestr[23] == '-':
            sgn = -1
        offseth = int(datestr[24:26])
        offsetm = int(datestr[27:29])
        offsets = offseth * 3600 + offsetm * 60
        #Get timezone name
        name = None
        if datestr.find('[') >= 0:
            name = datestr[datestr.find('[')+1:datestr.find(']')]
        #Create zoned datetime
        dt = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=tz.tzoffset(name, offsets))
    return dt
