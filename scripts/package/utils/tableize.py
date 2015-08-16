from sqlalchemy import Table, Column, Index, PrimaryKeyConstraint
from sqlalchemy import BigInteger, Boolean, Date, DateTime
from sqlalchemy import Float, Integer, String
# from sqlalchemy.schema import CreateTable, CreateIndex


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
    table = Table(table_name, metadata, *columns)
    return table
