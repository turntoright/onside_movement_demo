from tracing.core import Edge, trace


def by_id(results):
    return {r.property_id: r for r in results}


def test_chain_respects_max_hops():
    edges = [Edge("S", "A", 0, 10), Edge("A", "B", 20, 30)]
    one = by_id(trace(edges, "S", 0, 1))
    assert set(one) == {"S", "A"}
    two = by_id(trace(edges, "S", 0, 2))
    assert two["B"].path == ("S", "A", "B") and two["B"].hops == 2


def test_edges_must_depart_after_arrival_at_their_origin():
    edges = [Edge("S", "A", 0, 10), Edge("A", "B", 5, 30)]  # leaves A before infection arrives
    assert "B" not in by_id(trace(edges, "S", 0, 3))


def test_earliest_arrival_wins():
    edges = [Edge("S", "A", 0, 50), Edge("S", "B", 0, 5), Edge("B", "A", 6, 20)]
    result = by_id(trace(edges, "S", 0, 3))
    assert result["A"].arrival == 20
    assert result["A"].path == ("S", "B", "A")


def test_witness_path_is_consistent_with_hop_limit():
    # Review-2 counterexample: u improves in round 2 while v used u's round-1 value.
    edges = [
        Edge("S", "u", 0, 10),
        Edge("S", "x", 0, 2),
        Edge("x", "u", 3, 5),
        Edge("u", "v", 10, 20),
    ]
    result = by_id(trace(edges, "S", 0, 2))
    assert result["u"].arrival == 5 and result["u"].path == ("S", "x", "u")
    assert result["v"].arrival == 20 and result["v"].path == ("S", "u", "v")
    assert all(r.hops <= 2 for r in result.values())


def test_seed_without_edges_returns_only_the_seed():
    assert [r.property_id for r in trace([], "S", 0, 3)] == ["S"]
