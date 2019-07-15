from typing import Iterable, Iterator


def polling_intervals(
    start: Iterable[float], rest: float, max_duration: float = None
) -> Iterator[float]:
    def _intervals():
        yield from start
        while True:
            yield rest

    cumulative = 0.0
    for interval in _intervals():
        cumulative += interval
        if max_duration is not None and cumulative > max_duration:
            break
        yield interval
