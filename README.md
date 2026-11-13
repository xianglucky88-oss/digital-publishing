# 数字出版物元数据采集与版本追踪系统

该项目按单一职责拆分为：

- `sources.py`：读取 fixture 原始记录与 HTTP JSON 来源（分块暂存、流式校验、逐条产出，含条件下载）；
- `json_stream.py`：有界内存的增量 UTF-8 / JSON 流式解析（容器定位、深度与单条记录上限）；
- `http_cache.py`：验证符绑定字节快照的持久化缓存与原子提交；
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
PYTHONPATH=src python3 scripts/replay_conditional_cache.py
python3 scripts/make_streaming_fixtures.py          # rebuild fixtures/streaming doubles
PYTHONPATH=src python3 scripts/measure_streaming_memory.py
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

### Bounded-memory streaming reads

The HTTP source never decodes a whole response in memory. A read proceeds in
three stages with an observable publication boundary between them:

1. **Staging (temporary content).** The body is consumed in `chunk_size`
   chunks (default 64 KiB) into a temporary file while `max_bytes` (default
   64 MiB) is enforced and the SHA-256 is computed. A network interruption
   or a byte-limit violation deletes the partial file; nothing derived from
   it is ever published.
2. **Validation (snapshot publication).** The staged bytes are validated in
   one streaming pass: strict UTF-8 (incrementally decoded, so multibyte
   characters may straddle chunks), full JSON grammar, the container shape
   (top-level array, `records`/`data`/`items`/`results` by priority, or the
   `records_key` path) and a complete, garbage-free ending — content after
   the container is checked as strictly as the container itself. Only after
   this pass succeeds does the snapshot become observable: it is published
   to `raw_dir` and/or the conditional-download cache, and `last_report`
   describes it. A `last_report` therefore always certifies a fully
   validated snapshot.
3. **Import (formal observations).** Records are materialized one at a time
   from the validated snapshot and streamed into the pipeline. Until stage
   2 has completed, no parsed prefix is treated as a successful source
   batch, so a truncated or tampered tail can never leak partial records
   into the store.

Peak memory is bounded by `chunk_size`, one record (`max_record_chars`,
default 1 000 000 decoded characters) and `max_depth` (default 64) — it
does not grow with the number of records, and skipped fields (for example a
huge irrelevant metadata string) are validated without being materialized.

Limit violations are `ValueError`s and fail the run before anything is
imported, preserving all previously collected observations:

- total bytes beyond `max_bytes` — `source response exceeds byte limit …`;
- a single record spanning more than `max_record_chars` characters —
  `record exceeds size limit …` (raised while the record is being read, so
  the record is never fully materialized);
- nesting deeper than `max_depth` — `nesting exceeds depth limit …`.

All three limits accept constructor/`run_url` overrides (`max_bytes`,
`max_record_chars`, `max_depth`). If the caller stops consuming the
returned record stream early, `close()` (or the context-manager protocol,
used by `run_url`) releases the staging file deterministically; abandoned
streams are released on garbage collection. Existing successful data is
never touched by a failed or interrupted read.

`fixtures/streaming/` holds three same-prefix local response doubles (see
`scripts/make_streaming_fixtures.py`): a complete publication list, the
same bytes truncated only at the very tail, and the same records with a
multi-megabyte string in irrelevant metadata. `tests/test_streaming_source.py`
drives them through the real HTTP → staging → validation → pipeline path,
and `scripts/measure_streaming_memory.py` reports peak RSS for step-wise
increasing record counts (1k/8k/24k) plus the huge-single-record,
trailing-garbage, truncation, depth and byte-limit failure paths, each in a
fresh subprocess:

```bash
PYTHONPATH=src python3 scripts/measure_streaming_memory.py
```

### Conditional-download cache

Pass `--cache-dir` to reuse server validators across runs:

```bash
PYTHONPATH=src python3 -m digital_publishing.cli \
  --url https://example.invalid/api/records \
  --database /tmp/digital-publishing.sqlite3 \
  --cache-dir /tmp/digital-publishing-cache \
  --access-scope tenant-a
```

- The first read caches a `200` body together with its `ETag` and/or
`Last-Modified`. The next read sends `If-None-Match` / `If-Modified-Since`; a
`304` replays the exact snapshot the conditional request was derived from,
including to the pipeline, and reports `"downloaded_bytes": 0`. It never
returns an empty record list.
- Endpoints that send no validator are always read in full, and any stale
  validators for that identity are dropped.
- Only bytes that have been fully written and passed the existing JSON/container
  validation become cacheable. `raw_dir` still preserves the original bytes.
- The request identity covers the full URL (including query), `records_key`,
  the caller-declared `--access-scope`, and a one-way fingerprint of the
  effective credentials. Changing either scope or auth configuration therefore
  never reuses an old validator, and the index stores no credential values and
  no raw URL.
- `cache_dir/index.json` is committed with an atomic replace after the
  content-addressed blob has been fsynced. A crash between the two leaves an
  unreferenced blob and no index entry, so the next run falls back to a full
  read. A missing, truncated or corrupt index, or a deleted/tampered blob,
  also falls back to a full read; a `304` that cannot be resolved fails clearly.
- Interleaved writers are handled with a versioned entry: a `304` arriving
  after another writer advanced the identity returns the caller's validated
  bytes but does not roll the newer entry back. It is only re-bound when the
  entry still carries the exact digest *and* validators that were sent.
- CLI output adds `source_reads`: status, conditional/response headers,
  `snapshot_digest`, `record_count` and downloaded bytes per read. These are
  source-read facts only; they say nothing about collection state, sessions or
  power.

The cache layout is `blobs/<sha256>.json` (immutable response bodies) plus
`index.json` (validator-to-blob bindings). `ConditionalCache.prune_orphans()`
removes unreferenced blobs when no writer is active.

`scripts/replay_conditional_cache.py` reproduces the replay, interleaving,
container, validator, scope and restart/corruption cases against a scripted
offline server and prints the request-to-body evidence as JSON.

External dependency types and versions: Python 3.11+, pytest 8.3.x for tests. The HTTP adapter uses Python's standard library and adds no runtime package.
