from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData, Index, PrimaryKeyConstraint
from sqlalchemy import BigInteger, Boolean, Date, DateTime
from sqlalchemy import Float, Integer, String
from sqlalchemy.schema import CreateTable, CreateIndex

# creates a list schemas into a SQLalchemy thingy
def metadbize(table_metas, db_path = None):
    """
        `table_metas`: a list of {'name': 'table_name', 'schema': schemadict}

    returns a MetaData object
    """
    dbp = db_path if db_path else 'sqlite://'
    engine = create_engine(dbp)
    metadata = MetaData(engine)
    for tm in table_metas:
        tname, tmeta = tm
        tbl = tableize(tname, tmeta, metadata)
    return metadata

## INDICIES NOT IMPLEMENTED YET
def meta_to_sql(metadata):
    """ returns a String"""
    sql = []
    for t in metadata.tables.values():
        sql.append(str(CreateTable(t)).strip() + ';')
    return "\n".join(sql)
# turns one list and one table_schema into a SQLalchemy table
# returns a Table
def tableize(table_name, table_meta, metadata):
    # set up the columns
    columns = []
    for c_name, col_sch in table_meta['columns'].items():
        column = columnize(col_sch, c_name)
        columns.append(column)
    # set up the primary key
    pkeys = table_meta['db'].get('primary_key')
    if pkeys:
        pkeys = pkeys if type(pkeys) is list else [pkeys]
        pk = PrimaryKeyConstraint(*pkeys)
        columns.append(pk) # ugly...
    # initialize the table
    print("\n\n%s\n-------------" % table_name)
    table = Table(table_name, metadata, *columns)
    return table


def indexize(table_meta, table):
    indexes = []
    indexarr = table_meta['db'].get('indexes')
    if indexarr:
        for i in indexarr:
            arr = i if type(i) is list else [i]
            # give it a slugged name
            index_name = table_name + '-' + ('_'.join(arr))
            idx = Index(index_name, *[table.c[x] for x in arr])
            indexes.append(idx)
    return indexes


def columnize(col_meta, name = None):
    """
    `col_meta` is metadata representing a column to be made

    Returns: a Column object
    """
    c_name = name if name else col_meta['name']
    ct = col_meta['type']
    if ct in ['BigInteger', 'Boolean', 'Date', 'DateTime',
                'String', 'Float', 'Integer', ]:
        c_type = ct
    # elif ct == 'Enumerable':
    #     c_type = "Enum"
    else:
        raise Exception("Unrecognized column type:", ct)
    # now add optional params to the type
    if col_meta.get('length'):
        # arraify it
        lt = col_meta['length']
        lt = lt if isinstance(lt, list) else [lt]
        # convert to strings
        lt = [str(_x) for _x in lt]
        c_type = c_type + ("(%s)" % ','.join(lt))
    else:
        c_type = c_type + '()'
    # eval it
    try:
        c_type = eval(c_type)
    except:
        raise Exception("Failed to resolve column type: %s" % c_type)
    else:
        # set optional attributes
        atts = {}
        # column is nullable by default
        atts['nullable'] = False if col_meta.get('nullable') is False else True
        # make Column object
        return Column(c_name, c_type, **atts)
