# import signal
import time
import datetime
from signal import Signals
from typing import Callable, Dict, Generator

import pytz
# from canal.client import Client
from loguru import logger

from synch.common import insert_log
from synch.enums import ClickHouseEngine
from synch.factory import get_writer
from synch.reader import Reader
from synch.settings import Settings

from synch.replication.client import Client
from canal.protocol import EntryProtocol_pb2
from canal.protocol import CanalProtocol_pb2

from synch.convert import SqlConvert

len_event = 0
event_list = {}
is_insert = False
is_stop = False
last_insert_time = time.time()


class continuous():

    def __init__(self, alias):
        self.alias = alias
        self.source_db = Settings.get_source_db(alias)

        self.canal_host = Settings.get("canal", "host")
        self.canal_port = Settings.get("canal", "port")
        self.canal_username = Settings.get("canal", "username")
        self.canal_password = Settings.get("canal", "password")
        self.canal_client_id = Settings.get("canal", "client_id")
        self.canal_destination = Settings.get("canal", "destination")
        self.canal_filter = Settings.get("canal", "filter")

        self.client = Client()
        self.client.connect(host=self.canal_host, port=self.canal_port)
        self.client.check_valid(username=bytes(self.canal_username, 'utf-8'),
                                password=bytes(self.canal_password, 'utf-8'))
        self.client.subscribe(client_id=bytes(self.canal_client_id, 'utf-8'),
                              destination=bytes(self.canal_destination, 'utf-8'),
                              filter=bytes(self.canal_filter, 'utf-8'))

        self.server_id = self.source_db.get("server_id")
        self.skip_dmls = self.source_db.get("skip_dmls") or []
        self.skip_delete_tables = self.source_db.get("skip_delete_tables") or []
        self.skip_update_tables = self.source_db.get("skip_update_tables") or []
        self.databases = list(map(lambda x: x.get("database"), self.source_db.get("databases")))

    def sync(self, alias, target_schema, tables_pk, tables_nk, tables_tt, tables_dict, skip_error):
        for schema, table, event, message_id in self.canal_reading(
                target_schema=target_schema,
                skip_dmls=self.skip_dmls,
                skip_delete_tables=self.skip_delete_tables,
                skip_update_tables=self.skip_update_tables,
        ):
            # event["values"] = self.deep_decode_dict(event["values"])
            self.continuous_etl(
                alias, schema, target_schema, tables_pk, tables_nk, tables_tt, tables_dict, skip_error, table, event
            )
            # self.client.ack(message_id)
            logger.debug(f"success: key:{schema},event:{event},message_id:{message_id}")
            # self.after_continuous_etl(schema, table)


    def canal_reading(
            self,
            target_schema,
            skip_dmls,
            skip_delete_tables,
            skip_update_tables,
    ) -> Generator:
        while True:
            message = self.client.get_without_ack(1)
            message_id = message['id']

            # entries是每个循环周期内获取到数据集
            entries = message['entries']
            for entry in entries:
                entry_type = entry.entryType
                if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN,
                                  EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                    continue
                row_change = EntryProtocol_pb2.RowChange()
                row_change.MergeFromString(entry.storeValue)
                event_type = row_change.eventType
                if row_change.isDdl:
                    continue
                else:
                    # 根据增删改的其中一种情况进行数据处理
                    if event_type == EntryProtocol_pb2.EventType.DELETE:
                        header = entry.header
                        # 数据库名
                        schema = header.schemaName
                        event_unixtime = datetime.datetime.fromtimestamp(header.executeTime/1000).astimezone(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
                        # 表名
                        table = header.tableName
                        skip_dml_table_name = f"{schema}.{table}"
                        if "delete" in skip_dmls or skip_dml_table_name in skip_delete_tables:
                            continue

                        for row in row_change.rowDatas:
                            format_data = dict()
                            format_data['before'] = dict()
                            for column in row.beforeColumns:
                                format_data['before'][column.name] = column.value
                            event = {
                                "table": table,
                                "schema": schema,
                                "action": "delete",
                                "values": format_data['before'],
                                "event_unixtime": event_unixtime,
                                "action_seq": 1,
                            }
                            yield schema, table, event, message_id

                    elif event_type == EntryProtocol_pb2.EventType.INSERT:
                        header = entry.header
                        # 数据库名
                        schema = header.schemaName
                        event_unixtime = datetime.datetime.fromtimestamp(header.executeTime/1000).astimezone(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
                        # 表名
                        table = header.tableName
                        # raise Exception('TEST')
                        for row in row_change.rowDatas:
                            format_data = dict()
                            format_data['after'] = dict()
                            for column in row.afterColumns:
                                format_data['after'][column.name] = column.value
                            event = {
                                "table": table,
                                "schema": schema,
                                "action": "insert",
                                "values": format_data['after'],
                                "event_unixtime": event_unixtime,
                                "action_seq": 2,
                            }
                            yield schema, table, event, message_id
                    elif event_type == EntryProtocol_pb2.EventType.UPDATE:
                        header = entry.header
                        # 数据库名
                        schema = header.schemaName
                        event_unixtime = datetime.datetime.fromtimestamp(header.executeTime/1000).astimezone(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
                        # 表名
                        table = header.tableName
                        skip_dml_table_name = f"{schema}.{table}"
                        if "update" in skip_dmls or skip_dml_table_name in skip_update_tables:
                            continue

                        for row in row_change.rowDatas:
                            format_data = dict()
                            format_data['after'] = dict()
                            for column in row.afterColumns:
                                format_data['after'][column.name] = column.value
                            event = {
                                "table": table,
                                "schema": schema,
                                "action": "update",
                                "values": format_data['after'],
                                "event_unixtime": event_unixtime,
                                "action_seq": 3,
                            }
                            yield schema, table, event, message_id
                    else:
                        continue

            if message_id != 0:
                self.client.ack(message_id)
            time.sleep(1)

    def signal_handler(signum: Signals, handler: Callable):
        global is_stop
        global is_insert
        sig = Signals(signum)
        if len_event == 0:
            logger.info(f"shutdown on {sig.name} success")
            exit()
        else:
            logger.info(
                f"shutdown on {sig.name}, wait seconds at most to finish {len_event} events..."
            )
            is_stop = True
            is_insert = True


    def finish_continuous_etl(self):
        logger.info("finish success, bye!")
        exit()


    def continuous_etl(
        self, alias: str, schema: str, target_schema: str, tables_pk: dict, tables_nk: dict, tables_tt: dict, tables_dict: Dict, skip_error: bool, table: str, event: Dict,
    ):
        """
        continuous etl from canal and insert into clickhouse
        """
        global len_event
        global event_list
        global is_insert
        global last_insert_time

        # insert_interval = Settings.insert_interval()
        insert_num = Settings.insert_num()
        logger.info(f"start for {alias}.{schema}.{table} success")

        len_event += 1
        # table = event["table"]
        # schema = event["schema"]
        action = event["action"]
        # values = event["values"]
        # event_unixtime = event["event_unixtime"]

        engine = tables_dict.get(table).get("clickhouse_engine")
        writer = get_writer(engine)
        event_list = writer.handle_event(
            tables_nk, tables_dict, tables_pk.get(table), schema, table, action, event_list, event,
        )

        if (
            len_event >= insert_num
            or time.time() - last_insert_time >= Settings.insert_interval()
        ):
            is_insert = True

        if is_insert:
            for table, v in event_list.items():
                table_event_num = 0
                if isinstance(v, dict):
                    writer = get_writer(ClickHouseEngine.merge_tree)
                    insert = v.get("insert")
                    delete = v.get("delete")
                    update = v.get("update")
                    if insert:
                        insert = sorted(insert, key=lambda x: x.get("event_unixtime"))
                    if skip_error:
                        try:
                            if delete:
                                writer.delete_events(target_schema, table, delete, tables_nk.get(table), tables_tt.get(table))
                            if insert:
                                writer.insert_events(target_schema, table, insert, tables_nk.get(table), tables_tt.get(table))
                            if update:
                                writer.update_events(target_schema, table, update, tables_nk.get(table), tables_tt.get(table))

                        except Exception as e:
                            logger.error(
                                f"insert event error,error: {e}", exc_info=True, stack_info=True
                            )
                    else:
                        if delete:
                            writer.delete_events(target_schema, table, delete, tables_nk.get(table), tables_tt.get(table))
                        if insert:
                            writer.insert_events(target_schema, table, insert, tables_nk.get(table), tables_tt.get(table))
                        if update:
                            writer.update_events(target_schema, table, update, tables_nk.get(table), tables_tt.get(table))
                    if not delete:
                        delete = []
                    if not insert:
                        insert = []
                    if not update:
                        update = []
                    table_event_num += len(delete)
                    table_event_num += len(insert)
                    table_event_num += len(update)

                elif isinstance(v, list):
                    table_event_num += len(v)
                    writer = get_writer(ClickHouseEngine.collapsing_merge_tree)
                    if v:
                        if skip_error:
                            try:
                                writer.insert_events(target_schema, table, v, tables_nk.get(table), tables_tt.get(table))
                            except Exception as e:
                                logger.error(
                                    f"insert event error,error: {e}", exc_info=True, stack_info=True
                                )
                        else:
                            writer.insert_events(target_schema, table, v, tables_nk.get(table), tables_tt.get(table))

                insert_log(alias, schema, table, table_event_num, 2)

            logger.info(f"success commit {len_event} events")
            event_list = {}
            is_insert = False
            len_event = 0
            last_insert_time = time.time()
            if is_stop:
                self.finish_continuous_etl()
