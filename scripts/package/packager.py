from sqlalchemy import create_engine, MetaData
class DatabasePackager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.engine = create_engine(self.db_path)
        self.metadata = MetaData(self.engine)

    def tables(self):
        # via metadata
        # returns immutable collection
        return self.metadata

    def create_table(self, table_name, table_schema):
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
        table = Table(table_name, self.metadata, *columns)
        return table


    def load_data():
        return None
