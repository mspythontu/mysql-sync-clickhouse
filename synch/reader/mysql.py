import time
import mysql.connector
from signal import Signals
from typing import Callable, Tuple, Union

from loguru import logger


from synch.reader import Reader
from synch.settings import Settings


class Mysql(Reader):
    fix_column_type = True

    def __init__(self, alias):
        super().__init__(alias)
        source_db = self.source_db
        # self.conn = MySQLdb.connect(
        #     host=self.host,
        #     port=self.port,
        #     user=self.user,
        #     password=self.password,
        #     connect_timeout=10,
        #     cursorclass=DictCursor,
        #     charset="utf8",
        # )
        # self.conn.autocommit(True)
        # self.cursor = self.conn.cursor()
        # self.cursor.execute('set max_allowed_packet=67108864')
        self.conn = mysql.connector.connect(host=self.host,
            user=self.user,
            password=self.password,
            connect_timeout=10,
            charset="utf8")
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(buffered=True)
        # self.cursor.execute('set max_allowed_packet=67108864')


    def get_source_select_sql(self, schema: str, table: str, sign_column: str = None):
        select = "*"
        if sign_column:
            select += f", toInt8(1) as {sign_column}"
        return f"SELECT {select} FROM mysql('{self.host}:{self.port}', '{schema}', '{table}', '{self.user}', '{self.password}')"

    def get_binlog_pos(self) -> Tuple[str, str]:
        """
        get binlog pos from master
        """
        sql = "show master status"
        result = self.execute(sql)
        return result[0][0], result[0][1]

    def get_primary_key(self, db, table) -> Union[None, str, Tuple[str, ...]]:
        """
        get pk
        :param db:
        :param table:
        :return:
        """
        pri_sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}' and COLUMN_KEY='PRI'"
        result = self.execute(pri_sql)
        result = list(result)
        logger.debug(result)
        if not result:
            return None
        if len(result) > 1:
            return tuple(map(lambda x: x[0], result))
        return result[0][0]

    def get_order_by_key(self, db, table) -> Union[None, str, Tuple[str, ...]]:
        """
        get order by key
        :param db:
        :param table:
        :return:
        """
        order_by_sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}'"
        result = self.execute(order_by_sql)
        result = list(result)
        # print(result[0][0])
        if not result:
            return None
        return result[0][0]

    def get_all_columns(self, db, table) -> Union[None, str, Tuple[str, ...]]:
        """
        get all columns
        :param db:
        :param table:
        :return:
        """
        all_sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{db}' and TABLE_NAME='{table}'"
        result = self.execute(all_sql)
        result = list(result)
        # print(result[0][0])
        if not result:
            return None
        if len(result) > 1:
            return tuple(map(lambda x: x[0], result))
        return result[0][0]

    def get_columns_by_column_type_not_varchar(self, database, table) -> Union[None, str, Tuple[str, ...]]:
        """
        get columns by table column type not varchar
        :return:
        """
        sql = f"select COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME = '{table}' and COLUMN_TYPE not like '%varchar%' and IS_NULLABLE = 'yes' and TABLE_SCHEMA = '{database}'"
        result = self.execute(sql)
        result = list(result)
        if not result:
            return None
        if len(result) > 1:
            return tuple(map(lambda x: x[0], result))
        return result[0][0]

    def signal_handler(self, signum: Signals, handler: Callable):
        sig = Signals(signum)
        exit()

    def start_sync(self):
        log_file, log_pos = self.get_binlog_pos()
        log_pos = int(log_pos)
        logger.info(f"mysql binlog: {log_file}:{log_pos}")