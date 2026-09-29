import logging.handlers
import random
import sys
from typing import Dict, List, Union

from synch.enums import ClickHouseEngine, SourceDatabase
from synch.exceptions import ConfigurationError
from synch.reader import Reader
from synch.settings import Settings
from synch.writer import ClickHouse
from synch.writer.collapsing_merge_tree import ClickHouseCollapsingMergeTree
from synch.writer.merge_tree import ClickHouseMergeTree
from synch.writer.replacing_merge_tree import ClickHouseReplacingMergeTree
from synch.writer.versioned_collapsing_merge_tree import ClickHouseVersionedCollapsingMergeTree

_readers: Dict[str, Reader] = {}
_writers: Dict[str, List[ClickHouse]] = {}


def get_reader(alias: str) -> Reader:
    """
    get reader once
    """
    r = _readers.get(alias)
    if not r:
        source_db = Settings.get_source_db(alias)
        if not source_db:
            raise ConfigurationError(f"Can't find alias {alias} in config.")
        db_type = source_db.get("db_type")
        if db_type == SourceDatabase.mysql.value:
            from synch.reader.mysql import Mysql

            r = Mysql(alias)
        else:
            raise ConfigurationError(f"Unsupported db_type {db_type}")
        _readers[alias] = r
    return r


def get_writer(engine: ClickHouseEngine = None, choice=True) -> Union[ClickHouse, List[ClickHouse]]:
    """
    get writer once
    """
    writers = _writers.get(engine)
    if not choice:
        return writers
    if not writers:
        settings = Settings.get("clickhouse")
        host = settings.get("host")
        args = [host, settings.get("user"), settings.get("password")]
        if engine == ClickHouseEngine.merge_tree.value:
            w = ClickHouseMergeTree(*args)
        elif engine == ClickHouseEngine.collapsing_merge_tree.value:
            w = ClickHouseCollapsingMergeTree(*args)
        elif engine == ClickHouseEngine.versioned_collapsing_merge_tree.value:
            w = ClickHouseVersionedCollapsingMergeTree(*args)
        elif engine == ClickHouseEngine.replacing_merge_tree.value:
            w = ClickHouseReplacingMergeTree(*args)
        else:
            w = ClickHouseMergeTree(*args)
        _writers.setdefault(engine, []).append(w)
    return random.choice(_writers.get(engine))  # nosec:B311


def init_logging():
    """
    init logging config
    :param debug:
    :return:
    """
    base_logger = logging.getLogger("mysynch")
    debug = Settings.debug()
    if debug:
        base_logger.setLevel(logging.DEBUG)
    else:
        base_logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(fmt)
    base_logger.addHandler(sh)


def init_monitor_db():
    """
    init monitor db
    """
    writer = get_writer()
    sql_create_db = f"create database if not exists mysynch"
    writer.execute(sql_create_db)
    engine = "ReplacingMergeTree"
    sql_create_tb = f"""create table if not exists mysynch.log
(
    id         int,
    alias      String,
    schema     String,
    table      String,
    num        int,
    type       int,
    created_at DateTime
)
    engine = {engine} partition by toYYYYMM(created_at) order by id;"""
    writer.execute(sql_create_tb)


def init(config_file):
    """
    init
    """
    Settings.init(config_file)
    init_logging()
    if Settings.monitoring():
        init_monitor_db()
