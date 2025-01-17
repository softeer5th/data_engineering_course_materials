from app.core.interfaces import BaseExecutor


class Extractor(BaseExecutor[str, bool]):
    def execute(self, url: str) -> bool: ...
