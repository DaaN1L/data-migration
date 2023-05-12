import psycopg2 as pg2

from ..columns_info import ColumnsInfo
from ..common_queries import SELECT_COLUMNS_INFO_QUERY, SELECT_INCREMENT_QUERY
from ..driver_base import DriverBase


class DriverPostgresql(DriverBase):
    def __init__(self, host, port, database, username, password):
        self.conn = pg2.connect(
            dbname=database,
            user=username,
            password=password,
            host=host,
            port=port,
        )

    @property
    def name(self) -> str:
        return "postgresql"

    def get_columns_info(self, table: str, schema="public", include=None, exclude=None):
        if exclude is None:
            exclude = []
        with self.conn.cursor() as curs:
            curs.execute(SELECT_COLUMNS_INFO_QUERY.format(table=table, schema=schema))
            columns_info_raw = curs.fetchall()
        columns_info = []
        for column in columns_info_raw:
            if column[0] not in exclude and (include is None or column[0] in include):
                column_info = ColumnsInfo(*column)
                column_info.is_nullable = True if column_info.is_nullable == "YES" else "NO"
                column_info.default = "NULL" if column_info.default is None else column_info.default
                columns_info.append(column_info)
        return columns_info

    def select(self, table, columns, increment_key, from_, to_, schema="public"):
        columns_str = ", ".join(map(lambda x: f'"{x}"', columns))  # "x", "y", ...
        with self.conn.cursor() as curs:
            curs.execute(SELECT_INCREMENT_QUERY.format(
                table=table,
                schema=schema,
                columns=columns_str,
                increment=increment_key,
                from_=from_,
                to_=to_
            ))
            data = curs.fetchall()
        return data

    def insert(self):
        raise NotImplementedError

    def create_table(self):
        raise NotImplementedError

    def __del__(self):
        self.conn.close()


