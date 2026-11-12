# 数字出版物元数据采集与版本追踪系统

该项目按单一职责拆分为：

- `sources.py`：读取 fixture 原始记录；
- `normalize.py`：字段映射、标准化、质量校验和稳定去重键；
- `storage.py`：SQLite schema、幂等写入、拒绝记录和统计；
- `availability.py`：供货状态三态本地语义、声明时刻归一化与声明依据抽取；
- `claim_storage.py`：追加式声明依据的 SQLite 存储，同刻冲突双方保留、不覆盖；
- `adjudication.py`：独立的来源优先级裁决与状态时间线查询；
- `migration.py`：旧观测迁入声明表，只承认现存材料、不臆造已丢失冲突；
- `pipeline.py`：只编排依赖，不承担文件或 SQL 细节；
- `application.py`：组合依赖并管理资源生命周期；
- `cli.py`：命令行参数和结果输出。

## 数据工程链路

`fixture -> source adapter -> normalization -> deduplication -> quality checks -> SQLite sink`。

默认只使用本地 fixture，不连接真实站点，也不包含账号、连接串或 token。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）

## 运行

```bash
PYTHONPATH=src python3 -m digital_publishing.cli --fixture fixtures/sample.json --database /tmp/digital-publishing.sqlite3
PYTHONPATH=src python3 -m pytest tests
```

## HTTP source mode


The default fixture mode remains deterministic and credential-free. For a real JSON endpoint, use:

```bash
PYTHONPATH=src python3 -m digital_publishing.cli \
  --url https://example.invalid/api/records \
  --database /tmp/digital-publishing.sqlite3 \
  --raw-dir /tmp/digital-publishing-raw
```

Replace `digital_publishing` and `digital-publishing` with this repository's package/project name. The endpoint must return a JSON list, or an object containing `records`, `data`, `items`, or `results`. Use `--records-key` for a different nested path. Optional authentication is read from `DATA_SOURCE_BEARER_TOKEN` or `DATA_SOURCE_API_KEY`; do not commit their values. `--raw-dir` stores immutable content-addressed copies of successful HTTP responses. HTTP and JSON failures stop the run instead of being recorded as successful ingestion.

External dependency types and versions: Python 3.11+, pytest 8.3.x for tests. The HTTP adapter uses Python's standard library and adds no runtime package.

## 供货状态裁决（availability adjudication）

目录显示的可供货/已撤回状态不能由“最后一条被程序读到的记录”决定。系统在原有
观测入库之外，额外保存一份**追加式声明依据**（`availability_claims` 表，只有
INSERT，没有 UPDATE/DELETE），并提供独立的截止时刻裁决。

### 本地语义

- 状态三态：`available`（原始词 `active` 等）、`withdrawn`（原始词 `withdrawn`
  等）、`unknown`。缺失、空值或无法识别的取值一律 `unknown`，**绝不默认可供货**，
  也不推断销售订单或版权许可。
- 声明时刻沿用 `observed_at`：带时区偏移才归一化到 UTC 并进入时间线；无时区偏移
  或无法解析时材料保留但不参与裁决。
- 演示 ISBN（如 `978-1`）按普通字符串处理，不做真实 ISBN 校验位校验。
- 版次缺失的声明只作证据留存，**不自动作用到该 ISBN 的任何具体版次**。

### 裁决规则（显式来源优先级配置）

- 来源顺序由调用方以 `--priority a,b,c` 显式给出，靠前优先级更高；未配置来源同列
  最低档。
- 低优先来源的新声明不能盖过高优先来源仍有效的撤回。
- 同级来源给出矛盾结论时不能任取一条，结论为 `unknown`。
- 同一来源后续的明确恢复（如 withdrawn → active）可以改变自己的状态。
- 晚收到但声明时刻更早的材料按 `observed_at` 进入它所属的时间段，不会把当前状态
  倒退；时间线结论与入库到达顺序无关。
- 同来源、同产品版次、同一声明时刻出现不同内容时，双方都保留为冲突，该来源此时刻
  无法自证。旧 `observations` 表因覆盖式 upsert 已丢失的冲突，迁移时只承认材料不足，
  不做还原。

### 命令

```bash
# 入库（同时写观测表与追加式声明表）
PYTHONPATH=src python3 -m digital_publishing.cli \
  --database /tmp/digital-publishing.sqlite3 \
  ingest --fixture fixtures/sample.json

# 截止时刻裁决：各来源当时最新声明、优先规则依据、最终三态结论
PYTHONPATH=src python3 -m digital_publishing.cli \
  --database /tmp/digital-publishing.sqlite3 \
  adjudicate --isbn 978-1 --edition 2 \
  --as-of 2026-11-03T12:00:00Z --priority source-a,source-b

# 状态时间线：逐段列出 valid_from / valid_to / status / 裁决依据
PYTHONPATH=src python3 -m digital_publishing.cli \
  --database /tmp/digital-publishing.sqlite3 \
  timeline --isbn 978-1 --edition 2 --priority source-a,source-b

# 旧 observations 表迁入声明表（同库，幂等）
PYTHONPATH=src python3 -m digital_publishing.cli \
  --database /tmp/digital-publishing.sqlite3 migrate
```

标题、语言、出版者仍由 `normalize.py` 按原方式标准化保存；本能力不包含 ONIX
解析、源节点订正或消息导出。
