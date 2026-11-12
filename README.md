# 数字出版物元数据采集与版本追踪系统

该项目按单一职责拆分为：

- `sources.py`：读取 fixture 原始记录；
- `normalize.py`：字段映射、标准化、质量校验和稳定去重键；
- `storage.py`：SQLite schema、幂等写入、拒绝记录和统计；
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
