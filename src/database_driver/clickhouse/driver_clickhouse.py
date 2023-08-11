import clickhouse_connect

from .queries import CREATE_TABLE_QUERY
from ..columns_info import ColumnsInfo
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

    # @staticmethod
    # def _generate_columns_declaration(column_description: list[ColumnsInfo]) -> str:
    #     column_tmp = '"{name}" {dtype} {is_null} DEFAULT {default}'
    #     columns_declaration = [
    #         column_tmp.format(
    #             name=c.name,
    #             dtype=c.data_type,
    #             is_null="NULL" if c.is_nullable else "NOT NULL",  # TODO index
    #             default="NULL" if c.default is None else f"'c.default'"
    #         )
    #         for c in column_description
    #     ]
    #     return ", ".join(columns_declaration)

    def get_columns_info(self, table: str, schema="maindb", include=None, exclude=None):
        query = """
            SELECT column_name,
                   ordinal_position,
                   column_default,
                   is_nullable,
                   data_type
              FROM information_schema.columns
             WHERE table_schema = %{schema}s
               AND table_name = %{table}s
          ORDER BY ordinal_position
        """

        if exclude is None:
            exclude = []
        columns_info_raw = self.conn.query(query, {"schema": schema, "table": table}).result_rows
        columns_info = []
        for column in columns_info_raw:
            if column[0] not in exclude and (include is None or column[0] in include):
                column_info = ColumnsInfo(*column)
                column_info.is_nullable = True if column_info.is_nullable == "YES" else False
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
            # column_type_names=column_type_names,  # TODO add column_types
            column_names=column_names
        )

    def create_table(self, table, columns_description, pkey, schema="maindb"):
        columns = []
        names = {}
        defaults = {}
        for c in columns_description:
            names[f"col_name_{c.ordinal_position}"] = c.name
            defaults[f"col_name_{c.ordinal_position}"] = c.default
            columns.append(
                f'"{{col_name_{c.ordinal_position}:Identifier}}"'
                f" {c.data_type}"
                f" {'NULL' if c.is_nullable else 'NOT NULL'}"
                f" DEFAULT {{default_{c.ordinal_position}:{c.data_type}}}"
            )
        columns = ",\n".join(columns[1:2])
        query = f"""
            CREATE TABLE IF NOT EXISTS {{schema:Identifier}}.{{table:Identifier}}
            (
            Year String DEFAULT {{q:String}}
            )
            ENGINE = MergeTree
            ORDER BY {{index:Identifier}}
        """
        print(query)
        self.conn.command(
            query,
            parameters={"schema": schema, "q": "some_value", "table": table, "index": pkey, **names, **defaults,}
        )
        self.conn.command("SHOW CREATE TABLE {schema:Identifier}.{table:Identifier}", parameters={"schema": schema, "table": table})

    def __del__(self):
        self.conn.close()
