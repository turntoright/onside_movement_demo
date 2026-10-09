import random

from generator.config import SCALES
from generator.reference import PROPERTY_TYPES, ROLES, Reference, build_reference, haversine_km


def test_haversine_christchurch_to_lincoln_is_about_17_km():
    assert 16 < haversine_km(-43.5321, 172.6362, -43.6400, 172.4860) < 18


def test_build_reference_matches_scale_and_is_deterministic():
    scale = SCALES["ci"]
    ref = build_reference(scale, random.Random(1))
    assert len(ref.properties) == scale.n_properties
    assert len(ref.people) == scale.n_people
    assert ref == build_reference(scale, random.Random(1))
    assert {p.property_type for p in ref.properties} <= set(PROPERTY_TYPES)
    assert {p.role for p in ref.people} <= set(ROLES)


def test_two_poultry_properties_with_different_owners_always_exist():
    ref = build_reference(SCALES["aws"], random.Random(7))
    poultry_owners = {p.owner_company_id for p in ref.properties if p.property_type == "poultry"}
    assert len(poultry_owners) >= 2


def test_write_and_read_round_trip(tmp_path):
    ref = build_reference(SCALES["ci"], random.Random(1))
    ref.write(tmp_path)
    assert Reference.read(tmp_path) == ref
    assert (tmp_path / "properties.json").read_text().startswith("[")
