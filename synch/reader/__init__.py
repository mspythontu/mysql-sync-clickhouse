import abc
import json
# import signal
import time
# from signal import Signals
from typing import Callable, Tuple, Union

from loguru import logger

from synch.common import JsonEncoder, insert_log
from synch.settings import Settings


class Reader:
    cursor = None
    conn = None
    fix_column_type = False
    last_time = 0
    count = {}

    def __init__(self, alias: str):
        self.alias = alias
        source_db = Settings.get_source_db(alias)
        self.source_db = source_db
        self.host = source_db.get("host")
        self.port = source_db.get("port")
        self.user = source_db.get("user")
        self.password = source_db.get("password")

    def execute(self, sql, args=None):
        logger.debug(sql)
        self.cursor.execute(sql, args)
        ret = self.cursor.fetchall()
        return ret

    def get_count(self, schema: str, table: str):
        sql = f"select count(*) as c from {schema}.{table}"
        return self.execute(sql)[0][0]

    @abc.abstractmethod
    def get_primary_key(self, db: str, table: str) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_order_by_key(self, db, table) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_columns(self, db, table) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_columns_by_column_type_not_varchar(self, database, table) -> Union[None, str, Tuple[str, ...]]:
        raise NotImplementedError

    @abc.abstractmethod
    def start_sync(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_source_select_sql(self, schema: str, table: str, sign_column: str = None):
        raise NotImplementedError

    def deep_decode_dict(self, d: dict):
        ret = {}

        for k, v in d.items():
            if isinstance(k, bytes):
                k = k.decode()
            if isinstance(v, dict):
                ret[k] = json.dumps(self.deep_decode_dict(v), cls=JsonEncoder)
            elif isinstance(v, bytes):
                ret[k] = v.decode()
            else:
                ret[k] = v
        return ret

    def after_continuous_etl(self, schema, table):
        now = int(time.time())
        schema_table = f"{schema}.{table}"
        self.count.setdefault(schema_table, 0)
        self.count[schema_table] += 1
        if self.last_time == 0:
            self.last_time = now
        if now - self.last_time >= Settings.insert_interval():
            for schema_table, num in self.count.items():
                logger.info(f"success commit {num} events for {schema_table}")
                s, t = schema_table.split(".")
                insert_log(self.alias, s, t, num, 1)
            self.last_time = 0
            self.count = {}

    def get_binlog_pos(self):
        raise NotImplementedError
