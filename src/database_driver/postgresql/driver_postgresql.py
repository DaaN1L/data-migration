import psycopg2 as pg2
from psycopg2.sql import Identifier, SQL, Literal

from ..columns_info import ColumnsInfo
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
        query = SQL("""
            SELECT column_name,
                   ordinal_position,
                   column_default,
                   is_nullable,
                   data_type
              FROM information_schema.columns
             WHERE table_schema = %(schema)s
               AND table_name = %(table)s
          ORDER BY ordinal_position
        """)

        if exclude is None:
            exclude = []
        with self.conn.cursor() as curs:
            curs.execute(query, {"table": table, "schema": schema})
            columns_info_raw = curs.fetchall()
        columns_info = []
        for column in columns_info_raw:
            if column[0] not in exclude and (include is None or column[0] in include):
                column_info = ColumnsInfo(*column)
                column_info.is_nullable = True if column_info.is_nullable == "YES" else False
                columns_info.append(column_info)
        return columns_info

    def select(self, table, columns, increment_key, from_, to_, schema="public"):
        query = SQL("SELECT {fields} FROM {table_full_name} WHERE ").format(
            fields=SQL(', ').join(map(Identifier, columns)),
            table_full_name=Identifier(schema, table),
        )
        query += (SQL(" AND ")
                  .join([SQL("{} BETWEEN {} AND {}").format(Identifier(i), Literal(f), Literal(t))
                         for (i, f, t) in zip(increment_key, from_, to_)]))
        with self.conn.cursor() as curs:
            curs.execute(query)
            data = curs.fetchall()
        return data

    def insert(self,
               table: str,
               schema: str,
               columns_description: list[ColumnsInfo],
               values: list[tuple]):
        raise NotImplementedError

    def create_table(self,
                     table: str,
                     schema: str,
                     pkey: str,
                     columns_description: list[ColumnsInfo]):
        raise NotImplementedError

    def __del__(self):
        self.conn.close()
