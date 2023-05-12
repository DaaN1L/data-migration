import clickhouse_connect
from clickhouse_connect import common

from .queries import CREATE_TABLE_QUERY
from ..columns_info import ColumnsInfo
from ..common_queries import SELECT_COLUMNS_INFO_QUERY
from ..driver_base import DriverBase


class DriverClickhouse(DriverBase):
    def __init__(self, host, port, database, username, password):
        self.conn = clickhouse_connect.create_client(
            database=database,
            username=username,
            password=password,
            host=host,
            port=port,
        )

    @staticmethod
    def _generate_columns_declaration(column_description: list[ColumnsInfo]) -> str:
        column_tmp = '"{name}" {dtype} {is_null}'
        columns_declaration = [  # TODO DEFAULT
            column_tmp.format(name=c.name,
                              dtype=c.data_type,
                              is_null="NULL" if c.is_nullable and c.name != "index" else "NOT NULL",  # TODO index
                              )
            for c in column_description
        ]
        return ", ".join(columns_declaration)

    def get_columns_info(self, table: str, schema="maindb", include=None, exclude=None):
        if exclude is None:
            exclude = []
        columns_info_raw = self.conn.query(SELECT_COLUMNS_INFO_QUERY.format(table=table, schema=schema)).result_rows
        columns_info = []
        for column in columns_info_raw:
            if column[0] not in exclude and (include is None or column[0] in include):
                column_info = ColumnsInfo(*column)
                column_info.is_nullable = True if column_info.is_nullable == "YES" else "NO"
                column_info.default = "NULL" if column_info.default is None else column_info.default
        return columns_info

    @property
    def name(self) -> str:
        return "clickhouse"

    def select(self, table, schema, columns, increment_key, from_, to_):
        raise NotImplementedError

    def insert(self, table, columns_description, values, schema="maindb"):
        column_names = []
        column_type_names = []
        for column in columns_description:
            column_names.append(column.name)
            column_type_names.append(column.data_type)
        self.conn.insert(
            table=table,
            database=schema,
            data=values,
            # column_type_names=column_type_names,
            column_names=column_names
        )

    def create_table(self, table, columns_description, schema="maindb"):
        columns = self._generate_columns_declaration(columns_description)
        self.conn.command(CREATE_TABLE_QUERY.format(table=table, schema=schema, columns=columns))

    def __del__(self):
        self.conn.close()
