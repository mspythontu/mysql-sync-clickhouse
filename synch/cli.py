from loguru import logger

from synch.factory import get_reader, get_writer, init
from synch.replication.continuous import continuous
from synch.replication.etl import etl_full
from synch.settings import Settings

def cli(config: str):
    """
    Sync data from other DB to ClickHouse.
    """
    init(config)


def etl(alias: str, schema: str):
    tables_pk = {}
    tables_nk = {}
    tables = Settings.get_source_db_database_tables_name(alias, schema)
    reader = get_reader(alias)
    for table in tables:
        tables_pk[table] = reader.get_primary_key(schema, table)
        tables_nk[table] = reader.get_columns_by_column_type_not_varchar(schema, table)
    return reader, tables_pk, tables_nk, tables


def sync(alias: str, schema: str, target_schema: str, skip_error: bool):
    tables_dict = Settings.get_source_db_database_tables_dict(alias, schema)
    tables_tt = {}
    for item in tables_dict:
        tables_tt[item] = tables_dict[item].get("target_table")
    logger.info(f"start sync for {alias}")
    reader, tables_pk, tables_nk, tables = etl(alias, schema)
    reader.start_sync()
    continuous_sync = continuous(alias)
    continuous_sync.sync(alias, target_schema, tables_pk, tables_nk, tables_tt, tables_dict, skip_error)


def check(alias: str, schema: str, target_schema: str):
    reader = get_reader(alias)
    writer = get_writer()
    tables = Settings.get_source_db_database_tables_name(alias, schema)
    for table in tables:
        source_table_count = reader.get_count(schema, table)
        target_table_count = writer.get_count(target_schema, table)
        if source_table_count == target_table_count:
            logger.info(f"{schema}.{table} is equal, count={source_table_count}")
        else:
            logger.warning(
                f"{schema}.{table} is not equal, source_table_count={source_table_count}, target_table_count={target_table_count}"
            )


if __name__ == "__main__":
    # try:
    alias = "mysql_db"
    schema = "staff"
    target_schema = "staff"
    skip_error = False

    cli("../mysynch.yaml")
    sync(alias, schema, target_schema, skip_error)
        # check(alias, schema, target_schema)
    # except:
    #     logger.info("error!")
    #     exit()