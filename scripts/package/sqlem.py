from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData, Index
from sqlalchemy import BigInteger, Boolean, Date, DateTime
from sqlalchemy import Float, Integer, String
from sqlalchemy.schema import CreateTable, CreateIndex

# creates a list schemas into a SQLalchemy thingy
def dbize(table_schemas, db_path = None):
    """ returns a MetaData object"""
    dbp = db_path if db_path else 'sqlite://'
    engine = create_engine(dbp)
    metadata = MetaData(engine)
    for tschema in table_schemas:
        tbl = tableize(tschema, metadata)
    return metadata

## INDICIES NOT IMPLEMENTED YET
def get_raw_sql(metadata):
    """ returns a String"""
    sql = []
    for t in metadata.tables.values():
        sql.append(str(CreateTable(t)) + ';')
    return "\n".join(sql)
# turns one list and one table_schema into a SQLalchemy table
# returns a Table
def tableize(table_schema, metadata):
    table_name = table_schema['name']
    columns = []
    pkeys = table_schema['db'].get('primary_key')
    pkeys = table_schema if type(pkeys) is list else [pkeys]
    for c_name, col_sch in table_schema['columns'].items():
        column = columnize(col_sch, c_name)
        if column.name in pkeys:
            column.primary_key = True
        columns.append(column)
    # initialize the table
    table = Table(table_name, metadata, *columns)
    return table


def indexize(table_schema, table):
    indexes = []
    indexarr = table_schema['db'].get('indexes')
    if indexarr:
        for i in indexarr:
            arr = i if type(i) is list else [i]
            # give it a slugged name
            index_name = table_name + '-' + ('_'.join(arr))
            idx = Index(index_name, *[table.c[x] for x in arr])
            indexes.append(idx)
    return indexes


def columnize(col_schema, name = None):
    """
    `col_schema` is metadata representing a column to be made

    Returns: a Column object
    """
    c_name = name if name else col_schema['name']
    ct = col_schema['type']
    if ct in ['BigInteger', 'Boolean', 'Date', 'DateTime',
                'String', 'Float', 'Integer', ]:
        c_type = ct
    # elif ct == 'Enumerable':
    #     c_type = "Enum"
    else:
        raise Exception("Unrecognized column type:", ct)
    # now add optional params to the type
    if col_schema.get('length'):
        # arraify it
        lt = col_schema['length']
        lt = lt if isinstance(lt, list) else [lt]
        c_type = c_type + ("(%s)" % ','.join(lt))
    else:
        c_type = c_type + '()'
    # eval it
    c_type = eval(c_type)

    # set optional attributes
    atts = {}
    # column is nullable by default
    atts['nullable'] = False if col_schema.get('nullable') is False else True
    # make Column object
    return Column(c_name, c_type, **atts)
