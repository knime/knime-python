import datetime

def deserialize(bytes):
    durationstr = bytes.decode("utf-8")
    hidx = durationstr.find('H');
    hours = 0
    if hidx >= 0:
        hours = int(durationstr[:hidx])
        durationstr = durationstr[hidx+1:]
    midx = durationstr.find('M')
    minutes = 0
    if midx >= 0:
        minutes = int(durationstr[:midx])
        durationstr = durationstr[midx+1:]
    sidx = durationstr.find('S')
    seconds = 0
    millis = 0
    if sidx >= 0:
        pidx = durationstr.find('.')
        if pidx > 0:
            seconds = int(durationstr[:pidx])
            millistr = durationstr[pidx+1:sidx]
            while len(millistr) < 3:
                millistr += '0'
            millis = int(millistr)
            if seconds < 0:
                millis *= -1
        else:
            seconds = int(durationstr[:sidx])
    print('Decoded: ' + bytes.decode("utf-8") + ' to ' + str(datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds, milliseconds=millis)))
    return datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds, milliseconds=millis)
