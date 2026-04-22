from typing import Protocol, runtime_checkable
import networkx as nx


@runtime_checkable
class ClusteringMethod(Protocol):
    """Interface every clustering method must implement."""

    name: str

    def detect_communities(self, G: nx.Graph) -> list[set[str]]:
        """
        Given a graph G, return a list of communities.
        Each community is a set of node identifiers.
        Communities are sorted descending by size.
        """
        ...


_REGISTRY: dict[str, type] = {}


def register(cls):
    """Decorator: register a clustering method class."""
    _REGISTRY[cls.name] = cls
    return cls


def get_method(name: str) -> ClusteringMethod:
    """Instantiate and return the named clustering method."""
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown clustering method: {name!r}. "
            f"Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[name]()


def available_methods() -> list[str]:
    """Return names of all registered methods."""
    return list(_REGISTRY.keys())
