from sqlalchemy import BigInteger, Boolean, Date, DateTime
from sqlalchemy import Float, Integer, String


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
