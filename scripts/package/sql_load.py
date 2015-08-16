import csv
from sqlalchemy import Boolean, Date, DateTime, Float, BigInteger, Integer
from datetime import datetime

def create_db(db_path, mdb):
    """
    mdb is a MetaData object
    """
    ## ???

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

def get_field_transfoos(columns):
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

def load_data_table(mdb, table_name, csv_io):
    """
    table_name (str): points to an existing table in the mdb engine

    csv_io (io): is an open file handle to a CSV file with
        headers in the first line that match the
        target table's column names
    """
    engine = mdb.bind
    data = csv.DictReader(csv_io)

    table = mdb.tables[table_name]
    transformed_fields = get_field_transfoos(table.columns)

    # perform transformation if needed
    conn = engine.connect()
    arr = []
    for row in data:
        for fieldname, transfoo in transformed_fields.items():
            row[fieldname] = transfoo(row[fieldname])
        arr.append(row)
    if arr:
        result = conn.execute(table.insert(), arr)
        return result


