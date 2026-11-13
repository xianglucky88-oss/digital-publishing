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

## ONIX 3.0 参考名消息的接入与订正往返

`onix.py` / `onix_storage.py` / `onix_service.py` 在不改变 JSON 链路的前提下，
新增固定 ONIX 3.0 参考名（reference names）XML 子集的保真接入、投影与订正：

- 支持的结构：`ProductIdentifier(ProductIDType=15/03)/IDValue`（isbn）、
  `TitleDetail[TitleType=01]/TitleElement[TitleElementLevel=01]/TitleText`（title）、
  `Publisher[PublishingRole=01]/PublisherName`（publisher）、
  `DescriptiveDetail/EditionNumber`（edition，缺失不默认 1）、
  `Language[LanguageRole=01]/LanguageCode`（language）、
  `ProductSupply/SupplyDetail/ProductAvailability`（availability，保留原始代码）。
- 只有能唯一解释的字段才投影到六个目录观测字段；同角色多节点不拼接、不默认取第一项。
  必需字段缺失/有歧义的产品进入 rejected 并带明确原因；可选字段有歧义时留空并记录 issue，
  候选位置（元素路径 + 行号）随映射保存。
- 订正仅限 title/publisher/language，且必须能定位到唯一源节点；角色有歧义时返回
  `ambiguous` 和可选的原文位置，由调用方用 `--node-path` 显式选择后再提交。
- 每次提交必须带基准消息指纹（sha256）。基准已被新接入消息改变时返回 `conflict`，
  旧映射不会误改新版；订正落库是单事务（新版本、重投影、目录观测、订正状态一起提交），
  中途失败整体回滚，并另起事务保存 `failed` 订正记录与完整依据。
- 其余元素、重复结构、属性、注释、处理指令在保真 DOM 中原样保留，导出按语义重建
  （不要求字节级相同），原消息全文逐版本保存。

```bash
PYTHONPATH=src python3 -m digital_publishing.cli \
  --onix-import fixtures/onix/catalog_message.xml --database /tmp/onix.sqlite3
PYTHONPATH=src python3 -m digital_publishing.cli \
  --onix-plan "Example Publisher" --database /tmp/onix.sqlite3 \
  --field title --product-ref book-0001
PYTHONPATH=src python3 -m digital_publishing.cli \
  --onix-correct "Example Publisher" --database /tmp/onix.sqlite3 \
  --field title --value "Data Pipelines (Revised)" \
  --base-fingerprint <plan 返回的 fingerprint> --product-ref book-0001 \
  --rationale "编辑订正依据"
PYTHONPATH=src python3 -m digital_publishing.cli \
  --onix-export "Example Publisher" --database /tmp/onix.sqlite3 \
  --output /tmp/corrected.xml
```

`ambiguous` / `failed` / `conflict` 结果 CLI 以退出码 1 返回，JSON 证据仍完整打印。
本地演示消息位于 `fixtures/onix/`：`catalog_message.xml`（多产品、多语种、未映射扩展）、
`missing_edition.xml`（缺版次不凑主键）、`duplicate_roles.xml`（重复角色需显式选节点）、
`publisher_update.xml`（来源更新触发指纹冲突）。

### 无 pytest 环境下的本地校验

仓库只依赖标准库运行；测试声明依赖 pytest 8.3.x。若环境中没有 pytest，可用随仓库提供的
最小 shim 运行同一批测试（不替代正式 pytest）：

```bash
python3 -I tools/run_onix_tests.py
```

