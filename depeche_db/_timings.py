import contextlib as _contextlib
import time as _time
from collections import defaultdict as _defaultdict
from typing import Dict, List


class Timings:
    """
    Opt-in named-span timing helper.

    Disabled by default — the ``span`` context manager is a near no-op when
    ``enabled`` is False, so instances can be left attached to library
    internals without measurable overhead.

    When enabled, each ``with timings.span(name): ...`` records a wall-clock
    duration into ``samples[name]``. Names can be hierarchical strings
    (e.g. ``"add.insert_rows"``) — the helper itself doesn't impose a
    structure; the caller picks names that read well in a flat report.
    """

    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled
        self.samples: Dict[str, List[float]] = _defaultdict(list)

    @_contextlib.contextmanager
    def span(self, name: str):
        if not self.enabled:
            yield
            return
        t0 = _time.perf_counter()
        try:
            yield
        finally:
            self.samples[name].append(_time.perf_counter() - t0)

    def reset(self) -> None:
        self.samples = _defaultdict(list)
