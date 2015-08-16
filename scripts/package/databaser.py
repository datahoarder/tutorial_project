from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.schema import CreateTable, CreateIndex
from scripts.package.utils.converts import get_field_transforms
from scripts.package.utils.tableize import columnize, indexize, build_primary_key
from collections import defaultdict
class Databaser:
    def __init__(self, db_path):
        self.db_path = self.resolve_db_path(db_path)
        self.engine = create_engine(self.db_path)
        self.metadata = MetaData(self.engine)
        self._indexes = defaultdict(str)
        # self._table_metas = {}

    def indexes(self):
        # dictionary
        return self._indexes

    # def table_metas(self):
    #     # dictionary
    #     return self._table_metas

    def tables(self):
        # via metadata
        # returns immutable collection
        return self.metadata.tables


    def bootstrap_table(self, table_name, table_meta, data):
        self.create_table(table_name, table_meta)
        self.insert_data_into_table(table_name, data)

    def build_table(self, table_name, table_meta):
        # register table_metas
        # self.table_metas[table_name] = table_meta
        # set up the columns
        columns = []
        for c_name, col_meta in table_meta['columns'].items():
            column = columnize(col_meta, c_name)
            columns.append(column)
        # set up the primary keys
        pkeynames = table_meta['db'].get('primary_key')
        if pkeynames:
            pkey = build_primary_key(pkeynames)
            columns.append(pkey)
        # initialize the table
        table = Table(table_name, self.metadata, *columns)
        self.indexes()[table_name] = indexize(table, table_meta)
        return table

    def create_table(self, table_name, table_meta):
        table = self.build_table(table_name, table_meta)
        return table.create(self.engine)


    def insert_data_into_table(self, table_name, data):
        # data is a list of dicts
        table = self.tables()[table_name]
        # meta = self.table_metas()[table_name]
        transformed_fields = get_field_transforms(table.columns)
        # perform transformation if needed
        conn = self.engine.connect()
        arr = []
        for row in data:
            for fieldname, transfoo in transformed_fields.items():
                row[fieldname] = transfoo(row[fieldname])
            arr.append(row)
        if arr:
            result = conn.execute(table.insert(), arr)
            return result

    def resolve_db_path(self, path):
        return 'sqlite:///' + path if path else "sqlite://"


    def raw_schema_sql(self):
        """ returns a String"""
        sql = []
        for t in self.tables().values():
            sql.append(str(CreateTable(t)).strip() + ';')
        for idxes in self.indexes().values():
            for idx in idxes:
                sql.append(str(CreateIndex(idx)).strip() + ';')
        return "\n".join(sql)
