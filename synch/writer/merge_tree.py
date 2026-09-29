from typing import Dict, List, Union

from loguru import logger

from synch.enums import ClickHouseEngine
from synch.reader import Reader
from synch.writer import ClickHouse


class ClickHouseMergeTree(ClickHouse):
    engine = ClickHouseEngine.merge_tree

    def get_table_create_sql(
        self,
        reader: Reader,
        schema: str,
        target_schema: str,
        table: str,
        pk,
        partition_by: str = None,
        engine_settings: str = None,
        **kwargs,
    ):
        partition_by_str = ""
        engine_settings_str = ""
        if partition_by:
            partition_by_str = f" PARTITION BY {partition_by} "
        if engine_settings:
            engine_settings_str = f" SETTINGS {engine_settings} "
        select_sql = reader.get_source_select_sql(schema, table)
        return f"CREATE TABLE {target_schema}.{table} ENGINE = {self.engine} {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"

    def get_full_insert_sql(self, reader: Reader, schema: str, target_schema: str, table: str, sign_column: str = None):
        return f"insert into {target_schema}.{table} {reader.get_source_select_sql(schema, table, )}"

    def handle_event(
        self,
        tables_nk: Dict,
        tables_dict: Dict,
        pk,
        schema: str,
        table: str,
        action: str,
        tmp_event_list: Dict,
        event: Dict,
    ):
        values = self.pre_handle_values(tables_dict.get(table).get("skip_decimal"), event["values"])
        event["values"] = values
        tmp_event_list.setdefault(table, {}).setdefault(action, [])
        tmp_event_list[table][action].append(event)
        return tmp_event_list
