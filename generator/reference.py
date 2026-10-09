"""Reference data: properties, people, companies (spec §3.3, §3.6)."""

from __future__ import annotations

import json
import math
import random
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import Scale

PROPERTY_TYPES = (
    "poultry", "pork", "beef", "dairy", "kiwifruit", "vineyard", "office", "feed_mill",
)  # fmt: skip
TYPE_WEIGHTS = (0.22, 0.12, 0.14, 0.18, 0.12, 0.10, 0.06, 0.06)
REGIONS = {
    "canterbury": (-43.53, 172.64),
    "waikato": (-37.79, 175.28),
    "bay_of_plenty": (-37.88, 176.70),
    "hawkes_bay": (-39.49, 176.91),
    "marlborough": (-41.51, 173.95),
    "southland": (-46.41, 168.35),
}
TYPE_REGIONS = {
    "kiwifruit": ("bay_of_plenty", "waikato"),
    "vineyard": ("marlborough", "hawkes_bay"),
}
ROLES = {
    "poultry_contractor": ("poultry", "feed_mill"),
    "pork_vet": ("pork",),
    "livestock_carrier": ("beef", "dairy", "pork"),
    "dairy_contractor": ("dairy", "beef"),
    "orchard_worker": ("kiwifruit",),
    "vineyard_contractor": ("vineyard",),
    "consultant": PROPERTY_TYPES,
}
ROLE_WEIGHTS = {
    "poultry_contractor": 0.22,
    "pork_vet": 0.08,
    "livestock_carrier": 0.20,
    "dairy_contractor": 0.18,
    "orchard_worker": 0.12,
    "vineyard_contractor": 0.10,
    "consultant": 0.10,
}
NEAR_KM = 10.0


@dataclass(frozen=True)
class Property:
    property_id: str
    name: str
    property_type: str
    group_id: str
    owner_company_id: str
    region: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Person:
    person_id: str
    visitor_company_id: str
    role: str
    home_region: str


@dataclass(frozen=True)
class Company:
    company_id: str
    name: str
    kind: str


@dataclass(frozen=True)
class Reference:
    properties: tuple[Property, ...]
    people: tuple[Person, ...]
    companies: tuple[Company, ...]

    def property_map(self) -> dict[str, Property]:
        return {p.property_id: p for p in self.properties}

    def write(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        for name, rows in (
            ("properties", self.properties),
            ("people", self.people),
            ("companies", self.companies),
        ):
            (directory / f"{name}.json").write_text(json.dumps([asdict(r) for r in rows], indent=1))

    @classmethod
    def read(cls, directory: Path) -> Reference:
        def load(name: str) -> list[dict]:
            return json.loads((directory / f"{name}.json").read_text())

        return cls(
            tuple(Property(**r) for r in load("properties")),
            tuple(Person(**r) for r in load("people")),
            tuple(Company(**r) for r in load("companies")),
        )


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def build_reference(scale: Scale, rng: random.Random) -> Reference:
    owners = [
        Company(f"C{i:03d}", f"Owner {i}", "owner") for i in range(1, scale.n_owner_companies + 1)
    ]
    visitors = [
        Company(f"V{i:03d}", f"Contractor {i}", "visitor")
        for i in range(1, scale.n_visitor_companies + 1)
    ]
    types = [rng.choices(PROPERTY_TYPES, TYPE_WEIGHTS)[0] for _ in range(scale.n_properties)]
    types[0] = types[1] = "poultry"  # live breach pairs need two poultry sites, different owners
    properties = []
    for i, ptype in enumerate(types, start=1):
        owner = owners[0] if i == 1 else owners[1] if i == 2 else rng.choice(owners)
        region = rng.choice(TYPE_REGIONS.get(ptype, tuple(REGIONS)))
        lat0, lon0 = REGIONS[region]
        properties.append(
            Property(
                property_id=f"P{i:04d}",
                name=f"{ptype.replace('_', ' ').title()} {i}",
                property_type=ptype,
                group_id=f"{owner.company_id}-G{rng.randint(1, 3)}",
                owner_company_id=owner.company_id,
                region=region,
                # Spread measured so ~70% of moves are under 10 km at the ci and default
                # scales (spec §3.3); the tiny aws scale is sparser (~40%), which is accepted.
                lat=round(lat0 + rng.gauss(0, 0.06), 5),
                lon=round(lon0 + rng.gauss(0, 0.07), 5),
            )
        )
    roles, weights = list(ROLE_WEIGHTS), list(ROLE_WEIGHTS.values())
    people = [
        Person(
            person_id=f"U{i:05d}",
            visitor_company_id=rng.choice(visitors).company_id,
            role=rng.choices(roles, weights)[0],
            home_region=rng.choice(tuple(REGIONS)),
        )
        for i in range(1, scale.n_people + 1)
    ]
    return Reference(tuple(properties), tuple(people), tuple(owners + visitors))
