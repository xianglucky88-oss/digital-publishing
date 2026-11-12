from collections.abc import Callable, Iterable
from typing import Any
from .availability import extract_claim
from .models import Observation
from .storage import ObservationStore
from .claim_storage import ClaimStore


class IngestionPipeline:
    """只负责编排 source、normalizer 和 store，不读取文件也不定义 SQL。

    传入 ``claim_store`` 时，每条原始记录还会额外抽取一条追加式供货声明：
    该抽取与规范化相互独立，规范化被拒绝的记录（如缺 title/publisher 或缺
    edition）仍可作为声明依据留存，且同刻冲突不会被覆盖。
    """

    def __init__(
        self,
        records: Iterable[Any],
        normalizer: Callable[[Any], Observation],
        store: ObservationStore,
        claim_store: ClaimStore | None = None,
    ):
        self.records = records
        self.normalizer = normalizer
        self.store = store
        self.claim_store = claim_store

    def run(self) -> dict[str, int]:
        processed = changed = rejected = claims = claim_skips = 0
        for raw in self.records:
            processed += 1
            if self.claim_store is not None:
                claim = extract_claim(raw)
                if claim is None:
                    claim_skips += 1
                elif self.claim_store.append(claim):
                    claims += 1
            try:
                changed += int(self.store.upsert(self.normalizer(raw)))
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                self.store.reject(raw, str(exc))
        result = {
            "processed": processed,
            "changed": changed,
            "rejected": rejected,
            "claims_added": claims,
            "claims_skipped": claim_skips,
        }
        result.update(self.store.counts())
        if self.claim_store is not None:
            result.update(self.claim_store.counts())
        return result
