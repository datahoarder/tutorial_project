from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, Integer, String
from datetime import datetime

def convert_to_bool(s):
    if isinstance(s, bool):
        return s
    x = s.lower()
    if x == 'true' or x == 't':
        return True
    elif x == 'false' or x == 'f':
        return False
    else:
        raise ValueError("Could not convert `%s` to Boolean value" % s)

# attempts to convert numerical strings to numbers
# if not possibel
def convert_to_numeric(s, numtype):
    try:
        return numtype(s)
    except ValueError as e:
        if not s:
            return None
        else:
            raise e

def get_field_transforms(columns):
    arr = {}
    for col in columns:
        fieldname = col.name
        # todo: this needs to fit to the schema definition of date
        if isinstance(col.type, Boolean):
            arr[fieldname] = lambda v: convert_to_bool(v)
        if isinstance(col.type, Date):
            arr[fieldname] = lambda v: datetime.strptime(v, "%Y-%m-%d")
        elif isinstance(col.type, DateTime):
            arr[fieldname] = lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        elif type(col.type) in [Integer, BigInteger]:
            arr[fieldname] = lambda v: convert_to_numeric(v, int)
        elif isinstance(col.type, Float):
            arr[fieldname] = lambda v: convert_to_numeric(v, float)
    return arr
