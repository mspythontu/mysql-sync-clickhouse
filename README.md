# sundial-mysql-syn-clickhouse-balance

基于 **Canal** 订阅 MySQL binlog（ROW 模式）的增量同步工具，把变更事件以**追加写**的方式落到 ClickHouse，形成一张带 `update_ts` + `action_type`（`insert` / `update` / `delete`）的**变更轨迹（history）表**。

当前仓库实际在同步的业务表：`staff.sd_leave_staff_balance` → `staff.sd_leave_staff_balance_history`（员工请假余额变动流水）。

> 注意：它**不是**源表的实时镜像。每条 MySQL 的 DML 都会在 ClickHouse 侧留下**一行新的历史记录**（delete 也不例外，是写一条 `action_type='delete'` 的记录，而不是物理删除）。

---

## 目录

- [架构与数据流](#架构与数据流)
- [目录结构](#目录结构)
- [环境要求](#环境要求)
- [安装](#安装)
- [前置准备](#前置准备)
- [配置说明](#配置说明)
- [运行](#运行)
- [目标表结构要求](#目标表结构要求)
- [模块说明](#模块说明)
- [Docker](#docker)
- [已知限制与注意事项](#已知限制与注意事项)
- [安全提示](#安全提示)
- [许可证](#许可证)

---

## 架构与数据流

```
MySQL (ROW binlog)
      │  Canal Server 伪装为 MySQL slave 拉取 binlog
      ▼
Canal Server  :11111  (TCP, protobuf)
      │  canal-python 客户端 GET / ACK
      ▼
本工具 (synch)
      ├─ canal_reading()   解析 Entry → {table, schema, action, values, event_unixtime}
      ├─ continuous_etl()  按 insert_num / insert_interval 攒批
      └─ writer.insert/delete/update_events()  字符串拼 SQL
      ▼
ClickHouse :9000  (原生协议)
      └─ staff.sd_leave_staff_balance_history   (追加写 MergeTree)
```

**关键路径说明**

| 环节 | 实现方式 | 代码位置 |
| --- | --- | --- |
| binlog 来源 | 由外部 **Canal Server** 提供，本工具只做 Canal 的 consumer，**不直连 MySQL binlog** | `synch/replication/continuous.py:44-50` |
| 事件解析 | 反序列化 Canal protobuf `Entry` / `RowChange`，取 `beforeColumns`（delete）或 `afterColumns`（insert/update） | `synch/replication/continuous.py:87-173` |
| 攒批策略 | `len_event >= insert_num` 或 `距上次写入 >= insert_interval` 秒触发一次写入 | `synch/replication/continuous.py:227-231` |
| 写入语义 | **追加写**：每条事件拼一行 `insert into ...(cols..., update_ts, action_type) values (..., '<时间>', '<动作>')` | `synch/writer/__init__.py:105-107` |
| 位点提交 | 每处理完一个 message 就 `ack(message_id)`，位点由 Canal Server 侧保存 | `synch/replication/continuous.py:175-176` |

MySQL Reader（`synch/reader/mysql.py`）**只用于读取元数据**（主键、列名、列类型）和打印当前 binlog 位点，**不参与增量数据传输**。

---

## 目录结构

```
.
├── Dockerfile                     # 基础镜像，仅安装依赖
├── mysynch.yaml                   # 主配置文件
├── requirements.txt
├── LICENSE
├── README.md
└── synch/
    ├── __init__.py
    ├── cli.py                     # 入口：编排 sync / check
    ├── common.py                  # JsonEncoder + 监控打点 insert_log
    ├── convert.py                 # MySQL DDL → ClickHouse DDL（当前未接入）
    ├── enums.py                   # 源库类型 / ClickHouse 引擎枚举
    ├── exceptions.py
    ├── factory.py                 # Reader / Writer 工厂 + 日志 + 监控库初始化
    ├── settings.py                # YAML 配置加载与链式取值
    ├── reader/
    │   ├── __init__.py            # Reader 抽象基类
    │   └── mysql.py               # MySQL 元数据读取
    ├── replication/
    │   ├── client.py              # Canal 协议 TCP 客户端（connect / subscribe / get / ack）
    │   ├── continuous.py          # ★ 增量同步主循环
    │   └── etl.py                 # 全量 ETL（当前入口未调用）
    └── writer/
        ├── __init__.py            # ClickHouse 基类 + 事件落库 ★
        ├── merge_tree.py
        ├── collapsing_merge_tree.py
        ├── versioned_collapsing_merge_tree.py
        └── replacing_merge_tree.py
```

---

## 环境要求

| 组件 | 版本 / 说明 |
| --- | --- |
| Python | `3.8.10`（与 Dockerfile 一致；代码未用 3.9+ 语法） |
| MySQL | 5.7 / 8.0，需开启 ROW 模式 binlog |
| Canal Server | 阿里 Canal，开放 TCP 端口（默认 `11111`） |
| ClickHouse | 需开放**原生协议** `9000` 端口（`clickhouse_driver` 不走 HTTP 8123） |

依赖包见 `requirements.txt`，用途如下：

| 包 | 用途 |
| --- | --- |
| `loguru` | 全项目统一日志 |
| `mysql-connector-python` | **实际使用**的 MySQL 驱动（`reader/mysql.py`） |
| `mysqlclient` | 遗留依赖，连接代码已被注释，**当前未使用**（Windows 上易安装失败，可忽略） |
| `canal-python` | Canal TCP 客户端 + protobuf 协议定义 |
| `protobuf` | 锁定 `3.19.x`，**不可升到 4.x**（canal-python 不兼容） |
| `clickhouse_driver` | ClickHouse 原生协议写入 |
| `PyYAML` | 解析 `mysynch.yaml` |
| `mysqlparse` / `pyparsing` | `convert.py` 解析 DDL（当前未接入主流程） |
| `python-dateutil` | `common.py` 中的日期反序列化 |

> ⚠️ `pytz` 在 `synch/replication/continuous.py:7` 被使用，但**未写在 `requirements.txt`** 中，安装时需手动补上。

---

## 安装

```bash
# 1) 安装依赖（Windows 下 mysqlclient 编译失败可直接注释掉该行）
pip install -r requirements.txt

# 2) 补装缺失的 pytz
pip install pytz
```

Windows 提示：`mysqlclient~=2.1.0` 需要 VC++ 构建工具。由于它在本项目中**实际未被使用**（连接代码已经改成 `mysql.connector`），可以先把它从 `requirements.txt` 注释掉再安装。

---

## 前置准备

### 1. MySQL 侧

```ini
# my.cnf
server-id        = 1
log-bin          = mysql-bin
binlog_format    = ROW          # Canal 要求 ROW 模式
binlog_row_image = FULL         # 需要完整列值
```

Canal 拉 binlog 使用的账号需要：

```sql
CREATE USER 'canal'@'%' IDENTIFIED BY '...';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
```

本工具 `source_db.user` 使用的账号需要：`information_schema` 读权限 + 目标表 `SELECT` 权限；`show master status` 还需要 `REPLICATION CLIENT`。

### 2. Canal Server 侧

- 部署并启动 Canal Server，开放 TCP 端口；
- 创建 instance，其名称必须与配置中的 `canal.destination` 一致（当前为 `example2`）；
- instance 的过滤规则要与 `canal.filter` 对齐（当前为 `staff.sd_leave_staff_balance`）；
- `canal.client_id` 决定消费位点的隔离维度，多实例消费时不要重复。

### 3. ClickHouse 侧

- 开放原生协议 `9000` 端口；
- **目标库与目标表必须手工提前建好**。当前入口只调用 `sync()`，**不会自动建表/建库**（详见 [已知限制](#已知限制与注意事项)）；
- 目标表结构参见 [目标表结构要求](#目标表结构要求)；
- 若开启 `core.monitoring: true`，账号需具备建库权限（会自动创建 `mysynch.log`）。

---

## 配置说明

配置文件为 `mysynch.yaml`（**该文件不入库**，仓库只提供 `mysynch.yaml.example` 模板，首次使用请先复制）：

```bash
cp mysynch.yaml.example mysynch.yaml
```

下面给出结构与逐项说明：

```yaml
core:
  debug: true            # true 时打印每条 SQL（通过 writer.execute 的 logger.debug）
  insert_num: 1          # 攒够多少条事件触发一次批量写入
  insert_interval: 1     # 距上次写入多少秒触发一次批量写入
  monitoring: false      # true 时自动在 CH 建库 mysynch 并写入 mysynch.log 监控数据

source_db:
  - db_type: mysql       # 目前只支持 mysql
    alias: mysql_db      # 唯一标识，必须与入口传入的 alias 一致
    server_id: 1
    host: <mysql-host>
    port: 3306
    user: <mysql-user>
    password: <mysql-password>
    skip_dmls:           # 全局跳过的 DML 类型，可填 delete / update
    skip_delete_tables:  # 跳过 delete 的表，格式 schema.table
    skip_update_tables:  # 跳过 update 的表，格式 schema.table
    databases:
      - database: staff
        auto_create: false       # 可选，默认 true
        tables:
          - table: sd_leave_staff_balance
            skip_decimal: false  # 可选，默认 false
            auto_full_etl: false # 可选，默认 true
            clickhouse_engine: MergeTree
            partition_by:        # 可选，如 toYYYYMM(created_at)
            engine_settings:     # 可选，如 index_granularity=8192
            sign_column: sign    # Collapsing*/VersionedCollapsing 需要
            version_column:      # VersionedCollapsing / ReplacingMergeTree 需要
            target_table: sd_leave_staff_balance_history   # 目标表名，不配则与源表同名

target_db:
  database: staff

canal:
  host: '<canal-host>'
  port: 11111
  username: ''
  password: ''
  client_id: '1009'
  destination: 'example2'
  filter: 'staff.sd_leave_staff_balance'

clickhouse:
  host: <ch-host>:9000   # 必须带端口，代码按 ":" split
  user: default
  password: ''
```

### 参数细节与默认值

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `core.debug` | — | 仅影响 `writer.execute` 是否 `logger.debug(sql)` |
| `core.insert_num` | 需要显式配置 | 建议生产环境设 `20000` |
| `core.insert_interval` | 需要显式配置 | 建议生产环境设 `60`（秒） |
| `core.monitoring` | `false` | 开启后建 `mysynch.log`（`ReplacingMergeTree`）并记录每批事件数 |
| `source_db[].db_type` | — | 只支持 `mysql`，其它值抛 `ConfigurationError` |
| `skip_dmls` | `[]` | 元素可以是 `"delete"` 或 `"update"` |
| `auto_create` | `true` | **仅全量 ETL 路径用到**，见限制说明 |
| `skip_decimal` | `false` | `true` 时把 `Decimal` 转 `String`，规避 ClickHouse [issue #7690](https://github.com/ClickHouse/ClickHouse/issues/7690) |
| `auto_full_etl` | `true` | **仅全量 ETL 路径用到** |
| `clickhouse_engine` | `MergeTree` | 支持 `MergeTree` / `CollapsingMergeTree` / `VersionedCollapsingMergeTree` / `ReplacingMergeTree` |
| `target_table` | 源表名 | 改名能力，当前配置用到了 |
| `target_db.database` | — | ⚠️ **代码中从未读取**，真正的目标库由入口写死的 `"staff"` 决定 |

**务必注意**：`Settings` 各方法使用了 `@functools.lru_cache()`（`synch/settings.py:32`、`42`、`54`、`64`、`72`、`80`、`92`），配置在进程启动后**永久缓存**，改 YAML 必须重启进程。另外 `Settings.get()` 对不存在的键**静默返回 `None`**（`synch/settings.py:94-101`），拼错配置项不会报错。

---

## 运行

> ⚠️ **当前没有 CLI 框架**（无 `click` / `typer` / `argparse`），也没有 `--help`、`--alias` 之类的参数。`synch/cli.py` 只是一个普通模块加 `__main__` 块，`alias` / `schema` / `target_schema` / `skip_error` / 配置文件路径全部**硬编码**在其中。

```python
53:65:synch/cli.py
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
```

因为配置文件路径硬编码为 `"../mysynch.yaml"`，必须在 `synch/` 目录下执行：

```bash
cd synch
python cli.py
```

### 可用函数

| 函数 | 作用 |
| --- | --- |
| `cli(config)` | 初始化：加载配置 → 初始化日志 → 按需初始化监控库 |
| `etl(alias, schema)` | 收集每张表的主键 `tables_pk` 和「非 varchar 且可空列」`tables_nk` |
| `sync(alias, schema, target_schema, skip_error)` | **主流程**：打印 MySQL binlog 位点后进入 Canal 消费死循环 |
| `check(alias, schema, target_schema)` | 一致性校验：逐表比较源/目标 `count(*)`，不等则 `logger.warning` |

想跑一致性校验，可以把 `sync(...)` 注释掉、放开第 62 行（注意它有缩进错误，无多余的前导空格应当删除）。

`skip_error` 的含义：写 ClickHouse 失败时，是否只 `logger.error` 后继续（丢弃这批数据），而不是让进程崩溃。设为 `True` 会**静默丢数据**，生产环境建议 `False` 并配合告警。

---

## 目标表结构要求

写入侧会对列名做**白名单过滤**，`synch/writer/__init__.py:12-19` 硬编码了 `history_column_list`：

```python
12:19:synch/writer/__init__.py
history_column_list = set(('leave_balance_id', 'leave_type_id', 'staff_user_id', 'total_days', 'balance_days',
                              'update_time', 'balance', 'year', 'carried_forward_from_pre_year', 'earned_leave',
                              'earned_leave_pre_month', 'leave_unit', 'is_apply_at_a_time', 'earned_additional',
                              'taken', 'forfeit_to_year', 'carry_forward_to_next_year', 'refresh_month', 'is_available',
                              'is_manul_arned', 'remark', 'ytd_encashment', 'peroid_start_date', 'peroid_end_date',
                              'current_entitlement', 'full_year_entitlement', 'next_entitilement', 'is_refresh_round',
                              'pending_approval_days', 'pending_approval_weeks', 'update_ts', 'action_type', 'earned_leave_before_round',
                              'taken_adjustment', 'eligibility_adjustment'))
```

**只有在这个集合里的列才会被写入，其余列会被静默丢弃。** 源表结构变动时必须同步修改这段代码。

此外写入时固定追加 `update_ts`（事件发生时间，UTC）和 `action_type`，因此目标表必须包含这两列。参考 DDL：

```sql
CREATE DATABASE IF NOT EXISTS staff;

CREATE TABLE staff.sd_leave_staff_balance_history
(
    leave_balance_id             String,
    leave_type_id                Nullable(String),
    staff_user_id                Nullable(String),
    total_days                   Nullable(String),
    balance_days                 Nullable(String),
    update_time                  Nullable(String),
    balance                      Nullable(String),
    year                         Nullable(String),
    carried_forward_from_pre_year Nullable(String),
    earned_leave                 Nullable(String),
    earned_leave_pre_month       Nullable(String),
    leave_unit                   Nullable(String),
    is_apply_at_a_time           Nullable(String),
    earned_additional            Nullable(String),
    taken                        Nullable(String),
    forfeit_to_year              Nullable(String),
    carry_forward_to_next_year   Nullable(String),
    refresh_month                Nullable(String),
    is_available                 Nullable(String),
    is_manul_arned               Nullable(String),
    remark                       Nullable(String),
    ytd_encashment               Nullable(String),
    peroid_start_date            Nullable(String),
    peroid_end_date              Nullable(String),
    current_entitlement          Nullable(String),
    full_year_entitlement        Nullable(String),
    next_entitilement            Nullable(String),
    is_refresh_round             Nullable(String),
    pending_approval_days        Nullable(String),
    pending_approval_weeks       Nullable(String),
    earned_leave_before_round    Nullable(String),
    taken_adjustment             Nullable(String),
    eligibility_adjustment       Nullable(String),
    update_ts                    String,   -- 事件发生时间
    action_type                  String    -- insert / update / delete
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(update_ts))
ORDER BY (leave_balance_id, update_ts);
```

> 说明：由于写入层统一把所有值转成**字符串字面量**拼进 SQL（见下方限制第 2 条），上表列类型使用 `String` / `Nullable(String)` 是最省心的选择。若想用强类型，需要保证 MySQL 侧的字符串能被 ClickHouse 正确隐式转换。

**引擎选型建议**

- 既然是追加写流水表，用 **`MergeTree`** 即可；
- 若需要按主键取最新快照，可再建一张 **`ReplacingMergeTree`**（配 `version_column`）并用 `FINAL` 或 `argMax` 查询；
- **不建议使用 `CollapsingMergeTree`**：`collapsing_merge_tree.py:47-52` 里 delete/update/insert 三个分支全部赋 `sign = 1`，从不写 `-1`，折叠永远不会发生。

---

## 模块说明

### `synch/settings.py`

`Settings` 类通过 `yaml.safe_load` 一次性读入配置到类级 `_config` 字典，然后用 `get(*args)` 做链式取值（如 `Settings.get("core", "insert_num")`）。**没有 schema 校验、没有默认值填充、没有环境变量覆盖**，缺失键返回 `None`。

### `synch/factory.py`

- `get_reader(alias)`：按 `db_type` 分发创建 Reader，并缓存在模块级字典 `_readers`；
- `get_writer(engine)`：按引擎创建 Writer 单例，缓存在 `_writers`；
- `init_logging()`：⚠️ 配置的是名为 `"mysynch"` 的**标准库 logging logger**，但项目实际统一用 **loguru**，因此这份配置**不生效**；
- `init_monitor_db()`：`monitoring=true` 时创建 `mysynch.log` 表；
- `init(config_file)`：串联上面三步。

### `synch/reader/mysql.py`

用 `mysql.connector` 直连 MySQL，**只做元数据查询**：

| 方法 | 说明 |
| --- | --- |
| `get_primary_key` | 查 `information_schema.COLUMNS`，`COLUMN_KEY='PRI'`；单列返回字符串，复合返回元组 |
| `get_order_by_key` | 返回该表**第一列**（不是真正的排序列） |
| `get_all_columns` | 全部列名 |
| `get_columns_by_column_type_not_varchar` | `COLUMN_TYPE not like '%varchar%' and IS_NULLABLE='yes'`，用于空串转 NULL |
| `get_binlog_pos` | `show master status` |
| `start_sync` | 打印位点，**不建立任何 binlog dump 连接**（binlog 由 Canal 提供） |
| `get_source_select_sql` | 生成 ClickHouse `mysql()` 表函数的 SELECT，供全量 ETL 使用 |

### `synch/replication/client.py`

Canal 协议 TCP 客户端，封装了：

- `connect(host, port)` —— 握手校验；
- `check_valid(username, password)` —— 鉴权；
- `subscribe(client_id, destination, filter)` —— 先 `rollback(0)` 复位再订阅；
- `get_without_ack(batch_size, timeout, unit)` / `get(size)` / `ack(message_id)` / `rollback(batch_id)`。

文件末尾有一段可直接 `python client.py` 调试的 `__main__` 样例，会打印每条 rowChange 的 before/after 列，排 Canal 连通性问题时很有用。

### `synch/replication/continuous.py` ★ 核心

`canal_reading()` 是一个 Generator，主流程：

1. `while True: message = self.client.get_without_ack(1)` —— **每轮只取 1 条 message**；
2. 跳过 `TRANSACTIONBEGIN` / `TRANSACTIONEND`；
3. `row_change.isDdl` 为真直接 `continue`（**DDL 不同步**）；
4. 按事件类型处理：
   - `DELETE` → `action="delete"`，取 `beforeColumns`，`action_seq=1`，受 `skip_dmls` / `skip_delete_tables` 控制；
   - `INSERT` → `action="insert"`，取 `afterColumns`，`action_seq=2`，**没有跳过分支**；
   - `UPDATE` → `action="update"`，取 **`afterColumns`**（**丢弃了 before，拿不到旧值**），`action_seq=3`；
5. `ack(message_id)`，然后 `time.sleep(1)` —— **固定 1 秒轮询间隔**。

`continuous_etl()` 负责攒批与落库：按 `insert_num` / `insert_interval` 触发，取的 writer 由 `tables_dict[table]["clickhouse_engine"]` 决定；dict 结构时按 **delete → insert → update** 顺序写入，list 结构（Collapsing 路径）时整体写入；最后写监控日志并清空状态。

### `synch/writer/__init__.py` ★ 写入层

`ClickHouse` 基类封装 `execute` / `get_count` / `check_table_exists` / `check_database_exists` / `create_database` 等基础操作，以及三个事件写入方法 `insert_events` / `delete_events` / `update_events`（三者逻辑同构，仅末尾 `action_type` 不同）。

NULL 处理：只有当列属于 `tables_nk`（非 varchar 且可空）**且**值为空串时才写成 `null`，其余一律 `str(v).replace("'", "''")` 后加单引号。

### `synch/writer/*.py` 四个引擎子类

| 类 | 引擎 | `handle_event` 产出结构 |
| --- | --- | --- |
| `ClickHouseMergeTree` | `MergeTree` | `{table: {action: [event]}}`（dict） |
| `ClickHouseCollapsingMergeTree` | `CollapsingMergeTree(sign)` | `{table: [event]}`（list，sign 恒为 1） |
| `ClickHouseVersionedCollapsingMergeTree` | `VersionedCollapsingMergeTree(sign, version)` | 同父类 MergeTree |
| `ClickHouseReplacingMergeTree` | `ReplacingMergeTree(version)` | 未覆写，同父类 MergeTree |

`get_table_create_sql` 生成的是 `CREATE TABLE ... ENGINE=... ORDER BY {pk} AS mysql(...) limit 0`，即**利用 ClickHouse 的 `mysql()` 表函数直接从 MySQL 拉结构**；`get_full_insert_sql` 则是 `insert into ... select from mysql(...)`。这两条路径属于全量 ETL，当前入口未调用。

### `synch/convert.py`

MySQL DDL → ClickHouse DDL 转换器（`SqlConvert`），支持 `ADD COLUMN` / `DROP COLUMN` / `CHANGE COLUMN` / `MODIFY COLUMN`，含类型映射表。⚠️ 虽然被 `continuous.py:21` 导入，但因为 `isDdl` 分支直接 `continue`，**这段代码从未被执行**，且映射表里 `char -> FixedString` 未带长度，会生成非法 DDL。

### `synch/replication/etl.py`

全量 ETL 实现 `etl_full()`：按 `auto_create` 决定是否建库、按 `auto_full_etl` 决定是否灌数据、`renew=True` 时先 `DROP TABLE`。⚠️ 在 `cli.py:5` 只是被 import，**从未被调用**。

### `synch/common.py`

`JsonEncoder` / `object_hook` 用于 JSON 往返；`insert_log(alias, schema, table, num, type_)` 在 `monitoring` 开启时往 `mysynch.log` 写一行打点。

---

## Docker

```dockerfile
FROM python:3.8.10
USER root:root
WORKDIR /app
#COPY . /app
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
```

构建：

```bash
docker build -t synch-mysql-clickhouse:latest .
```

运行：

```bash
docker run -d --name synch \
  -v ${PWD}:/app \
  -w /app/synch \
  synch-mysql-clickhouse:latest \
  python cli.py
```

> ⚠️ Dockerfile 第 4 行 `#COPY . /app` 被注释掉了，镜像里**没有任何源码和配置文件**，也没有 `CMD` / `ENTRYPOINT` / `EXPOSE`。因此必须挂载源码卷并显式指定命令；又因为 `cli.py:60` 硬编码 `"../mysynch.yaml"`，工作目录必须是 `synch/`（`-w /app/synch`）。

---

## 已知限制与注意事项

> 这些都是通读代码后确认的现状，动手改之前建议先读一遍。

### 入口与调用链

1. **没有 CLI 参数**：`alias` / `schema` / `target_schema` / `skip_error` / 配置路径全部硬编码在 `cli.py:53-61`，没有环境变量入口。
2. `cli.py:62` 存在缩进错误（当前被注释，不影响运行）。
3. **全量 ETL 是死代码**：`etl_full` 被 import 但从未调用。因此 `auto_create` / `auto_full_etl` / `skip_decimal` / `partition_by` / `engine_settings` / `sign_column` / `version_column` 这些配置项**在当前运行路径下全部无效**（`etl.py:67` 注释里提到的 `--renew` 参数也不存在）。
4. `target_db.database` 在代码中从未被读取。
5. `check()` 用源表名去查目标表（`cli.py:44`），配了 `target_table` 时会查错表。

### 数据正确性

6. **列白名单硬编码**：`writer/__init__.py:12-19` 写死 35 个业务列名，不在名单里的列静默丢弃。这是本项目与 `sd_leave_staff_balance` 表最强的耦合点，改源表必须改代码。
7. **SQL 完全靠 f-string 拼接**，只做了单引号转义，`synch/writer/__init__.py:94`：

   ```python
   94:95:synch/writer/__init__.py
   insert_dict[key] = str(insert_dict[key]).replace("\'", "\'\'")
   insert_value = insert_value + f"'{insert_dict[key]}'" + ','
   ```

   没有参数化绑定，也没有转义 `\`、`\n` 等；值中含反斜杠或换行会出问题，存在注入/破坏面。
8. **批量拼接 bug**：`insert_events` / `delete_events` / `update_events` 中 `key_value` 与 `insert_value` 在函数入口初始化一次后，**在 `for insert_dict in insert_dicts` 循环内没有重置**（`writer/__init__.py:84-85`、`87`）。单批多于一行时列名会重复累加，生成畸形 SQL。默认值 `insert_num=1` 恰好掩盖了这个问题——**调大 `insert_num` 之前必须先修这个 bug**。
9. `CollapsingMergeTree` 语义失效：`collapsing_merge_tree.py:47-52` 三种 action 全都赋 `sign = 1`，永远不写 `-1`。
10. UPDATE 事件丢弃 `beforeColumns`（`continuous.py:158-162`），拿不到变更前的值。
11. INSERT 没有对应的 skip 分支，无法被 `skip_dmls` / `skip_update_tables` 过滤。
12. 若 Canal filter 比 YAML 配置宽，`tables_dict.get(table)` 返回 `None`，`continuous.py:221` 会抛 `AttributeError`。
13. `ReplacingMergeTree` 子类未覆写 `handle_event`，且 `continuous_etl` 的 dict 分支强制用 `MergeTree` writer 写入（`continuous.py:237`），去重能力实际不生效。

### 吞吐与容错

14. **吞吐很低**：每轮只 `get_without_ack(1)`（`continuous.py:82`）且固定 `time.sleep(1)`（`:177`），上限约每秒一批。
15. 默认 `insert_num=1` / `insert_interval=1` 意味着每行提交一次 INSERT，ClickHouse 侧会迅速产生大量小 part，很快触发 **"too many parts"** 错误。生产应按建议改为 `20000 / 60`。
16. **没有重试、没有断点持久化**：ACK 在 `canal_reading:175-176` 于每条 message 处理后立即提交；若后续写 ClickHouse 失败且 `skip_error=True`，数据就直接丢了。
17. **无优雅退出**：`signal_handler`（`:179-191`）与 `finish_continuous_etl`（`:194-196`）从未注册（`import signal` 在 `:1` 被注释），`is_stop` 恒为 `False`。Ctrl+C / kill 时内存中的 `event_list` 会丢失。

### 配置与运维

18. `Settings.get()` 对缺失键静默返回 `None`（`settings.py:94-101`），配置拼错不会报错。
19. `factory.init_logging()` 配置的标准库 logger 名为 `"mysynch"`，与项目实际使用的 loguru 无关，**完全不生效**。
20. `pytz` 缺失于 `requirements.txt`（靠传递依赖侥幸可用）；`mysqlclient` 已无用却仍在依赖里。
21. DDL 同步完全关闭（`convert.py` 从未执行）。
22. Dockerfile 没有 `COPY . /app`，镜像不可独立运行。
23. `Settings` 的 `lru_cache` 是进程级永久缓存，改配置必须重启。

---

## 安全提示

🔴 **仓库历史中曾提交过真实的数据库连接地址与明文密码**（`source_db[0].password`、`canal` 段、`clickhouse` 段）。虽然 `mysynch.yaml` 现已移出版本控制并改为提供 `mysynch.yaml.example` 模板、历史也已重建，但**凭据一旦进入过任何远端仓库就必须视为已泄露**。

建议立即：

1. **轮换**这些凭据（MySQL 同步账号密码，以及 Canal / ClickHouse 的访问凭据）；
2. 本地的 `mysynch.yaml` 已被 `.gitignore` 忽略，请勿强行 `git add -f`；
3. 敏感信息建议通过环境变量或密钥管理服务注入，可改写 `synch/settings.py` 支持环境变量覆盖。

---

## 许可证

本项目基于 [MIT License](LICENSE) 开源。
