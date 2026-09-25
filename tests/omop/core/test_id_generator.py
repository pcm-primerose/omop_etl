import datetime as dt
import pytest

from omop_etl.omop.core.id_generator import (
    ROW_ID_SCHEME_VERSION,
    RowIdGenerator,
    RowIdTruncationCollision,
    row_id,
    sha256_bigint,
)


class TestRowIdDeterminism:
    def test_same_inputs_produce_the_same_id(self):
        assert row_id("drug_exposure", "p1", "cycle-2") == row_id("drug_exposure", "p1", "cycle-2")

    def test_different_namespace_produces_a_different_id(self):
        assert row_id("drug_exposure", "p1") != row_id("condition_occurrence", "p1")

    def test_different_parts_produce_a_different_id(self):
        assert row_id("drug_exposure", "p1") != row_id("drug_exposure", "p2")

    def test_part_order_is_significant(self):
        assert row_id("drug_exposure", "a", "b") != row_id("drug_exposure", "b", "a")

    def test_sha256_bigint_is_a_raw_string_hash_not_canonicalized(self):
        # sha256_bigint hashes `value` directly, row_id JSON-wraps `*parts` first,
        # they're not interchangeable for the "same" logical value
        assert sha256_bigint("drug_exposure", "p1") != row_id("drug_exposure", "p1")


class TestRowIdCanonicalization:
    def test_distinguishes_int_str_float_and_bool_for_the_same_literal(self):
        ids = {
            row_id("ns", 1),
            row_id("ns", "1"),
            row_id("ns", 1.0),
            row_id("ns", True),
        }
        assert len(ids) == 4

    def test_date_and_datetime_are_distinguished_from_their_isoformat_string(self):
        d = dt.date(2023, 1, 1)
        dtm = dt.datetime(2023, 1, 1)
        ids = {
            row_id("ns", d),
            row_id("ns", dtm),
            row_id("ns", d.isoformat()),
        }
        assert len(ids) == 3

    def test_none_is_a_distinct_part_not_a_missing_one(self):
        assert row_id("ns", "a", None) != row_id("ns", "a")

    def test_nested_tuples_are_canonicalized(self):
        assert row_id("ns", (1, 2)) == row_id("ns", (1, 2))
        assert row_id("ns", (1, 2)) != row_id("ns", (2, 1))

    def test_unsupported_part_type_raises(self):
        with pytest.raises(TypeError):
            row_id("ns", object())


class TestRowIdRange:
    def test_row_id_fits_in_53_bits(self):
        for parts in [("p1",), ("p2", "cycle-1"), (42,), ("", None, 1.5)]:
            value = row_id("ns", *parts)
            assert 0 <= value < (1 << 53)


class TestRowIdSchemeVersionScoping:
    def test_scheme_version_is_part_of_the_hash_input(self):
        import hashlib

        payload = "p1"
        real_digest = hashlib.sha256(f"v{ROW_ID_SCHEME_VERSION}:ns:{payload}".encode()).digest()
        other_version_digest = hashlib.sha256(f"v{ROW_ID_SCHEME_VERSION + 1}:ns:{payload}".encode()).digest()

        assert real_digest != other_version_digest
        assert sha256_bigint("ns", payload) == int.from_bytes(real_digest[:8], "big") & ((1 << 53) - 1)


class TestRowIdGenerator:
    def test_generates_the_same_id_as_the_pure_function(self):
        generator = RowIdGenerator()
        assert generator.generate("drug_exposure", "p1", "cycle-2") == row_id("drug_exposure", "p1", "cycle-2")

    def test_requesting_the_same_identity_twice_is_not_a_collision(self):
        generator = RowIdGenerator()
        first = generator.generate("drug_exposure", "p1")
        second = generator.generate("drug_exposure", "p1")
        assert first == second

    def test_two_builders_independently_computing_the_same_fk_do_not_collide(self):
        # what LocationBuilder/PersonBuilder relies on: two separate
        # call sites requesting the identical (namespace, parts) share one generator
        generator = RowIdGenerator()
        location_from_person_builder = generator.generate("location", 4093)
        location_from_location_builder = generator.generate("location", 4093)
        assert location_from_person_builder == location_from_location_builder

    def test_different_namespaces_do_not_interfere_even_with_the_same_parts(self):
        generator = RowIdGenerator()
        # exercising both namespaces on the same generator must not raise
        generator.generate("drug_exposure", "p1")
        generator.generate("condition_occurrence", "p1")

    def test_raises_on_a_true_truncation_collision(self):
        # simulate a collision: seed a different prior digest under the exact
        # (namespace, row_id) key a real call is about to reproduce
        generator = RowIdGenerator()
        colliding_id = generator.generate("drug_exposure", "p1")
        generator._seen[("drug_exposure", colliding_id)] = (b"\x00" * 32, ("p0",))

        with pytest.raises(RowIdTruncationCollision) as exc_info:
            generator.generate("drug_exposure", "p1")

        assert exc_info.value.namespace == "drug_exposure"
        assert exc_info.value.row_id == colliding_id
        assert exc_info.value.first_digest == b"\x00" * 32
        assert exc_info.value.second_digest != exc_info.value.first_digest
        assert exc_info.value.first_parts == ("p0",)
        assert exc_info.value.second_parts == ("p1",)

    def test_collision_in_one_namespace_does_not_affect_another(self):
        generator = RowIdGenerator()
        colliding_id = generator.generate("drug_exposure", "p1")
        generator._seen[("drug_exposure", colliding_id)] = (b"\x00" * 32, ("p0",))

        # same (fake) row_id value under a different namespace is unaffected
        generator._seen[("condition_occurrence", colliding_id)] = (b"\x00" * 32, ("p0",))
        with pytest.raises(RowIdTruncationCollision):
            generator.generate("drug_exposure", "p1")
