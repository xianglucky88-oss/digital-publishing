from collections.abc import Callable, Iterable
from typing import Any
from .models import Observation
from .storage import ObservationStore


class IngestionPipeline:
    """只负责编排 source、normalizer 和 store，不读取文件也不定义 SQL。"""

    def __init__(self, records: Iterable[Any], normalizer: Callable[[Any], Observation], store: ObservationStore):
        self.records = records
        self.normalizer = normalizer
        self.store = store

    def run(self) -> dict[str, int]:
        processed = changed = rejected = 0
        for raw in self.records:
            processed += 1
            try:
                changed += int(self.store.upsert(self.normalizer(raw)))
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                self.store.reject(raw, str(exc))
        result = {"processed": processed, "changed": changed, "rejected": rejected}
        result.update(self.store.counts())
        return result
