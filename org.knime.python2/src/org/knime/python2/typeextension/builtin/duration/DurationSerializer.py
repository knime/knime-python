def serialize(timedelta_obj):
    microstr = str(timedelta_obj.microseconds // 1000)
    while len(microstr) < 3:
        microstr = '0' + microstr
    durationstr = str(timedelta_obj.days * 24 + timedelta_obj.seconds // 3600) + 'H ' + str((timedelta_obj.seconds % 3600) // 60) + 'm ' + str(timedelta_obj.seconds % 60) + '.' + microstr + 's'
    print('Serializing timedelta: ' + str(timedelta_obj) + ' as ' + durationstr)
    return durationstr.encode("utf-8")