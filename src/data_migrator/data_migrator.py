from typing import Any
from copy import copy

from .types_map import TYPES_MAP
from ..database_driver import DriverBase
from ..database_driver import ColumnsInfo


# TODO: add duplicates checking, ask about memory efficiency
class DataMigrator:
    def __init__(self):
        self.source_db: DriverBase | None = None
        self.target_db: DriverBase | None = None
        self.type_map: dict[str, str] | None = None

    def _set_type_map(self) -> None:
        map_str = f"{self.source_db.name}_{self.target_db.name}"
        self.type_map = TYPES_MAP.get(map_str, None)
        if self.type_map is None:
            raise NotImplemented(
                f"Missing type mapping between {self.source_db.name} and {self.target_db.name} databases"
            )

    def _map_types(self, columns_description: list[ColumnsInfo]) -> list[ColumnsInfo]:
        mapped_columns_description = []
        for column in columns_description:
            mapped_column = copy(column)
            mapped_column.data_type = self.type_map[column.data_type]
            mapped_columns_description.append(mapped_column)
        return mapped_columns_description

    def migrate(
            self,
            source_db: DriverBase,
            source_schema: str,
            source_table: str,
            target_db: DriverBase,
            target_schema: str,
            target_table: str,
            target_pkey: str,
            increment_key: str | list[str],
            load_from: Any,
            load_to: Any,
            include_columns: None | list = None,
            exclude_columns: None | list = None,
    ):
        self.source_db = source_db
        self.target_db = target_db
        self._set_type_map()
        columns_description = self.source_db.get_columns_info(
            table=source_table,
            schema=source_schema,
            include=include_columns,
            exclude=exclude_columns
        )
        mapped_column_description = self._map_types(columns_description)
        self.target_db.create_table(
            table=target_table,
            schema=target_schema,
            columns_description=mapped_column_description,
            pkey=target_pkey,
        )
        # data = self.source_db.select(
        #     table=source_table,
        #     schema=source_schema,
        #     columns=[c.name for c in columns_description],
        #     increment_key=increment_key,
        #     from_=load_from,
        #     to_=load_to
        # )
        # self.target_db.insert(
        #     table=target_table,
        #     schema=target_schema,
        #     columns_description=mapped_column_description,
        #     values=data
        # )
