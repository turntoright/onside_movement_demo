"""Hop-limited earliest-arrival tracing over time-respecting edges (spec §5 Tracing)."""

from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

INF = float("inf")


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    departure: float
    arrival: float


@dataclass(frozen=True)
class Reached:
    property_id: str
    arrival: float
    hops: int
    path: tuple[str, ...]


def trace(edges: Iterable[Edge], seed: str, start: float, max_hops: int) -> list[Reached]:
    out: dict[str, list[Edge]] = defaultdict(list)
    for edge in edges:
        if edge.arrival >= edge.departure:
            out[edge.src].append(edge)
    for lst in out.values():
        lst.sort(key=lambda e: e.departure)
    departures = {src: [e.departure for e in lst] for src, lst in out.items()}

    arr: dict[str, float] = {seed: start}
    improved_at: list[dict[str, str]] = [{}]  # improved_at[r][v] = predecessor in layer r-1
    frontier = {seed}
    for _ in range(max_hops):
        previous = dict(arr)  # arr_{r-1}: every relaxation this round reads it
        best: dict[str, tuple[float, str]] = {}
        for u in sorted(frontier):
            lst = out.get(u)
            if not lst:
                continue
            for edge in lst[bisect_left(departures[u], previous[u]) :]:
                if edge.arrival >= previous.get(edge.dst, INF):
                    continue
                current = best.get(edge.dst)
                if current is None or (edge.arrival, u) < current:
                    best[edge.dst] = (edge.arrival, u)
        if not best:
            break
        layer = {}
        for v, (arrival, u) in best.items():
            arr[v] = arrival
            layer[v] = u
        improved_at.append(layer)
        frontier = set(best)

    final = len(improved_at) - 1

    def path_to(v: str) -> tuple[str, ...]:
        nodes = [v]
        layer = final
        while True:
            found = next((k for k in range(layer, 0, -1) if v in improved_at[k]), 0)
            if found == 0:
                break
            v = improved_at[found][v]
            nodes.append(v)
            layer = found - 1
        return tuple(reversed(nodes))

    results = []
    for v, arrival in arr.items():
        path = path_to(v)
        results.append(Reached(v, arrival, len(path) - 1, path))
    return sorted(results, key=lambda r: (r.arrival, r.property_id))
