import abc
from copy import deepcopy
from decimal import Decimal
from typing import Dict, List, Union, Tuple

import clickhouse_driver
from loguru import logger

from synch.enums import ClickHouseEngine
from synch.reader import Reader

history_column_list = set(('leave_balance_id', 'leave_type_id', 'staff_user_id', 'total_days', 'balance_days',
                              'update_time', 'balance', 'year', 'carried_forward_from_pre_year', 'earned_leave',
                              'earned_leave_pre_month', 'leave_unit', 'is_apply_at_a_time', 'earned_additional',
                              'taken', 'forfeit_to_year', 'carry_forward_to_next_year', 'refresh_month', 'is_available',
                              'is_manul_arned', 'remark', 'ytd_encashment', 'peroid_start_date', 'peroid_end_date',
                              'current_entitlement', 'full_year_entitlement', 'next_entitilement', 'is_refresh_round',
                              'pending_approval_days', 'pending_approval_weeks', 'update_ts', 'action_type', 'earned_leave_before_round',
                              'taken_adjustment', 'eligibility_adjustment'))

class ClickHouse:
    engine: ClickHouseEngine
    len_event = 0
    is_stop = False
    is_insert = False
    event_list = {}

    def __init__(self, host: str, user: str, password: str):
        host, port = host.split(":")
        self._client = clickhouse_driver.Client(host=host, port=port, password=password, user=user)

    def get_count(self, schema: str, table: str):
        sql = f"select count(*) as c from {schema}.{table}"
        return self.execute(sql)[0][0]

    def check_table_exists(self, schema: str, table: str):
        sql = f"select 1 from system.tables where database = '{schema}' and name = '{table}'"
        ret = self.execute(sql)
        if ret:
            return bool(ret[0][0])
        return False

    def check_database_exists(self, schema: str):
        sql = f"select 1 from system.databases where name = '{schema}'"
        ret = self.execute(sql)
        if ret:
            return bool(ret[0][0])
        return False

    def create_database(self, schema: str):
        sql = f"create database if not exists {schema}"
        return self.execute(sql)

    def execute(self, sql, params=None, *args, **kwargs):
        logger.debug(sql)
        logger.debug(params)
        return self._client.execute(sql, params, *args, **kwargs)

    def fix_table_column_type(
        self, reader, database, target_database, table,
    ):
        """
        fix table column type in full etl
        :return:
        """
        sql = f"select COLUMN_NAME, COLUMN_TYPE,IS_NULLABLE from information_schema.COLUMNS where TABLE_NAME = '{table}' and COLUMN_TYPE like '%decimal%'and TABLE_SCHEMA = '{database}'"
        cursor = reader.cursor
        cursor.execute(sql)
        logger.debug(sql)
        ret = cursor.fetchall()
        # cursor.close()
        for item in ret:
            column_name = item[0]
            is_nullable = item[2]
            column_type = item[1]
            if is_nullable:
                fix_sql = f"alter table {target_database}.{table} modify column {column_name} Nullable({column_type})"
            else:
                fix_sql = f"alter table {target_database}.{table} modify column {column_name} {column_type}"
            self.execute(fix_sql)

    def insert_events(self, target_schema: str, table: str, insert_data: List[Dict], table_null: Union[None, str, Tuple[str, ...]], target_table: str):
        insert_dicts = list(map(lambda x: dict(values=x.get("values"), event_unixtime=x.get("event_unixtime")), insert_data))
        insert_value = ''
        key_value = ''
        global history_column_list
        for insert_dict in insert_dicts:
            event_unixtime = insert_dict["event_unixtime"]
            insert_dict = insert_dict["values"]
            for key in insert_dict.keys():
                if key in history_column_list:
                    key_value = key_value + key + ','
                    if not table_null:
                        insert_dict[key] = str(insert_dict[key]).replace("\'", "\'\'")
                        insert_value = insert_value + f"'{insert_dict[key]}'" + ','
                    elif isinstance(table_null, tuple) and key in table_null and str(insert_dict[key]) == '':
                        insert_value = insert_value + 'null' + ','
                    elif key == table_null and str(insert_dict[key]) == '':
                        insert_value = insert_value + 'null' + ','
                    else:
                        insert_dict[key] = str(insert_dict[key]).replace("\'", "\'\'")
                        insert_value = insert_value + f"'{insert_dict[key]}'" + ','
            # key_value = key_value[:-1]
            # insert_value = insert_value[:-1]
            key_value = key_value + 'update_ts,action_type'
            insert_value = insert_value + f"'{event_unixtime}'" + ',\'insert\''
            sql = f"insert into {target_schema}.{target_table}({key_value}) values ({insert_value})"
            # return sql
            logger.debug("sql=" + sql)
            self.execute(sql)

    def delete_events(self, target_schema: str, table: str, insert_data: List[Dict], table_null: Union[None, str, Tuple[str, ...]], target_table: str):
        insert_dicts = list(map(lambda x: dict(values=x.get("values"), event_unixtime=x.get("event_unixtime")), insert_data))
        insert_value = ''
        key_value = ''
        global history_column_list
        for insert_dict in insert_dicts:
            event_unixtime = insert_dict["event_unixtime"]
            insert_dict = insert_dict["values"]
            for key in insert_dict.keys():
                if key in history_column_list:
                    key_value = key_value + key + ','
                    if not table_null:
                        insert_dict[key] = str(insert_dict[key]).replace("\'", "\'\'")
                        insert_value = insert_value + f"'{insert_dict[key]}'" + ','
                    elif isinstance(table_null, tuple) and key in table_null and str(insert_dict[key]) == '':
                        insert_value = insert_value + 'null' + ','
                    elif key == table_null and str(insert_dict[key]) == '':
                        insert_value = insert_value + 'null' + ','
                    else:
                        insert_dict[key] = str(insert_dict[key]).replace("\'", "\'\'")
                        insert_value = insert_value + f"'{insert_dict[key]}'" + ','
            # key_value = key_value[:-1]
            # insert_value = insert_value[:-1]
            key_value = key_value + 'update_ts,action_type'
            insert_value = insert_value + f"'{event_unixtime}'" + ',\'delete\''
            sql = f"insert into {target_schema}.{target_table}({key_value}) values ({insert_value})"
            # return sql
            logger.debug("sql=" + sql)
            self.execute(sql)

    def update_events(self, target_schema: str, table: str, insert_data: List[Dict], table_null: Union[None, str, Tuple[str, ...]], target_table: str):
        insert_dicts = list(map(lambda x: dict(values=x.get("values"), event_unixtime=x.get("event_unixtime")), insert_data))
        insert_value = ''
        key_value = ''
        global history_column_list
        for insert_dict in insert_dicts:
            event_unixtime = insert_dict["event_unixtime"]
            insert_dict = insert_dict["values"]
            for key in insert_dict.keys():
                if key in history_column_list:
                    key_value = key_value + key + ','
                    if not table_null:
                        insert_dict[key] = str(insert_dict[key]).replace("\'", "\'\'")
                        insert_value = insert_value + f"'{insert_dict[key]}'" + ','
                    elif isinstance(table_null, tuple) and key in table_null and str(insert_dict[key]) == '':
                        insert_value = insert_value + 'null' + ','
                    elif key == table_null and str(insert_dict[key]) == '':
                        insert_value = insert_value + 'null' + ','
                    else:
                        insert_dict[key] = str(insert_dict[key]).replace("\'", "\'\'")
                        insert_value = insert_value + f"'{insert_dict[key]}'" + ','
            # key_value = key_value[:-1]
            # insert_value = insert_value[:-1]
            key_value = key_value + 'update_ts,action_type'
            insert_value = insert_value + f"'{event_unixtime}'" + ',\'update\''
            sql = f"insert into {target_schema}.{target_table}({key_value}) values ({insert_value})"
            logger.debug("sql="+sql)
            # return sql
            self.execute(sql)

    @abc.abstractmethod
    def get_table_create_sql(
        self,
        reader: Reader,
        schema: str,
        target_schema: str,
        table: str,
        pk,
        partition_by: str,
        engine_settings: str,
        **kwargs,
    ):
        raise NotImplementedError

    @abc.abstractmethod
    def get_full_insert_sql(self, reader: Reader, schema: str, target_schema: str, table: str, sign_column: str = None):
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    def pre_handle_values(self, skip_decimal: bool, values: Dict):
        """
        handle decimal column if skip_decimal
        """
        tmp_values = deepcopy(values)
        if skip_decimal:
            for k, v in values.items():
                if isinstance(v, Decimal):
                    tmp_values[k] = str(v)
            return tmp_values
        return values