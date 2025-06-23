from typing import Iterable

import datetime as dt

import pytest
import psycopg
from assertical.fixtures import postgres as pg_fixtures

from envoy.admin.crud import certificate as cert_crud
from envoy.server.model import base as base_model


@pytest.mark.anyio
async def test_count_certificates_for_aggregator(pg_base_config: psycopg.Connection) -> None:
    async with pg_fixtures.generate_async_session(pg_base_config) as session:
        assert (await cert_crud.count_certificates_for_aggregator(session, aggregator_id=1)) == 3


@pytest.mark.parametrize(
    "agg_id,expected_certs",
    [
        (1, [1, 2, 3]),
        (2, [4]),
        (3, [5]),
    ],
)
@pytest.mark.anyio
async def test_select_all_certificates_for_aggregator(
    pg_base_config: psycopg.Connection, agg_id: int, expected_certs: list[int]
) -> None:
    async with pg_fixtures.generate_async_session(pg_base_config) as session:
        certs = await cert_crud.select_all_certificates_for_aggregator(session, agg_id, 0, 100)
        assert all([isinstance(c, base_model.Certificate) for c in certs])
        assert expected_certs == [c.certificate_id for c in certs]


@pytest.mark.parametrize(
    "certs,expected",
    [
        ([(1, None), (2, None), (3, None), (4, None), (5, None), (6, None)], [1, 2, 3, 4, 5]),
        ([(1, None), (None, "403ba02aa36fa072c47eb3299daaafe94399adad"), (1, None)], [1, 2]),
        ([(654664, "NOT_AN_LFDIarstoianerstoien")], []),
    ],
)
@pytest.mark.anyio
async def test_bulk_retrieve_certificates_by_id_or_lfdi(
    pg_base_config: psycopg.Connection, certs: list[tuple[int | None, str | None]], expected: list[int]
) -> None:
    async with pg_fixtures.generate_async_session(pg_base_config) as session:
        cert_ass_reqs = [base_model.Certificate(certificate_id=cert[0], lfdi=cert[1]) for cert in certs]
        actual = await cert_crud.select_many_certificates_by_id_or_lfdi(session, cert_ass_reqs)
        assert all([isinstance(a, base_model.Certificate) for a in actual])
        assert expected == [a.certificate_id for a in actual]


@pytest.mark.parametrize(
    "start,limit,expected_certs",
    [
        (0, 10, [1, 2, 3, 4, 5]),
        (1, 10, [2, 3, 4, 5]),
        (1, 3, [2, 3, 4]),
        (10, 20, []),
    ],
)
@pytest.mark.anyio
async def test_select_all_certificates(
    pg_base_config: psycopg.Connection, start: int, limit: int, expected_certs: list[int]
) -> None:
    async with pg_fixtures.generate_async_session(pg_base_config) as session:
        actual = await cert_crud.select_all_certificates(session, start, limit)
        assert all([isinstance(a, base_model.Certificate) for a in actual])
        assert expected_certs == [a.certificate_id for a in actual]


@pytest.mark.anyio
async def test_create_many_certificates(pg_empty_config: psycopg.Connection) -> None:
    async with pg_fixtures.generate_async_session(pg_empty_config) as session:
        created = dt.datetime.now()
        certificates = [
            base_model.Certificate(lfdi="SOMELFDI1", created=created, expiry=dt.datetime(2026, 6, 16, 1, 2, 3)),
            base_model.Certificate(lfdi="SOMELFDI2", created=created, expiry=dt.datetime(2029, 6, 16, 1, 2, 3)),
        ]
        await cert_crud.create_many_certificates(session, certificates) is None

        contents = await cert_crud.select_all_certificates(session, 0, 5000)
        assert [(1, "SOMELFDI1"), (2, "SOMELFDI2")] == [(c.certificate_id, c.lfdi) for c in contents]


@pytest.mark.anyio
async def test_create_many_certificates_on_conflict_do_nothing(pg_empty_config: psycopg.Connection) -> None:
    async with pg_fixtures.generate_async_session(pg_empty_config) as session:
        # Create certificates
        expiry_two = dt.datetime(2029, 6, 16, 1, 2, 3, tzinfo=dt.timezone.utc)

        certificates = [
            base_model.Certificate(lfdi="SOMELFDI1", expiry=dt.datetime(2026, 6, 16, 1, 2, 3)),
            base_model.Certificate(lfdi="SOMELFDI2", expiry=expiry_two),
        ]
        await cert_crud.create_many_certificates(session, certificates)

        certs = await cert_crud.select_all_certificates(session, 0, 500)

        original_created = next((c for c in certs if c.lfdi == "SOMELFDI2")).created

        # Create more using new function with same lfdis as before
        new_created = dt.datetime.now()
        new_certificates = [
            base_model.Certificate(lfdi="SOMELFDI2", created=new_created, expiry=dt.datetime(2126, 6, 17, 1, 2, 3)),
            base_model.Certificate(lfdi="SOMELFDI3", created=new_created, expiry=dt.datetime(2129, 6, 17, 1, 2, 3)),
            base_model.Certificate(lfdi="SOMELFDI4", created=new_created, expiry=dt.datetime(2132, 6, 17, 1, 2, 3)),
        ]

        result = await cert_crud.create_many_certificates_on_conflict_do_nothing(session, new_certificates)

        # Confirm only new ones created with no duplicate lfdis
        assert len(result) == 2, "Transaction should return newly created only"
        contents = await cert_crud.select_all_certificates(session, 0, 5000)
        # A skip in primary key occurs due to the attempted adding of the conflicting lfdi
        assert [(1, "SOMELFDI1"), (2, "SOMELFDI2"), (4, "SOMELFDI3"), (5, "SOMELFDI4")] == [
            (c.certificate_id, c.lfdi) for c in contents
        ]

        # Confirm existing LFDI not returned
        assert len([r for r in result if r.lfdi == "SOMELFDI2"]) == 0

        # Confirm existing LFDI not updated
        lfdi_twos = [c for c in contents if c.lfdi == "SOMELFDI2"]
        assert len(lfdi_twos) == 1
        lfdi_two = lfdi_twos[0]
        assert lfdi_two.created == original_created
        assert lfdi_two.expiry == expiry_two


@pytest.mark.anyio
async def test_create_many_certificates_on_conflict_do_nothing_empty_iter(pg_empty_config: psycopg.Connection) -> None:
    async with pg_fixtures.generate_async_session(pg_empty_config) as session:
        # Create certificates
        created = dt.datetime.now(tz=dt.timezone.utc)
        expiry_two = dt.datetime(2029, 6, 16, 1, 2, 3, tzinfo=dt.timezone.utc)

        certificates = [
            base_model.Certificate(lfdi="SOMELFDI1", created=created, expiry=dt.datetime(2026, 6, 16, 1, 2, 3)),
            base_model.Certificate(lfdi="SOMELFDI2", created=created, expiry=expiry_two),
        ]
        await cert_crud.create_many_certificates(session, certificates)

        # Create more using new function with same lfdis as before
        input: list[base_model.Certificate] = []
        new_certificates: Iterable[base_model.Certificate] = (i for i in input)

        result = await cert_crud.create_many_certificates_on_conflict_do_nothing(session, new_certificates)

        # Confirm only new ones created with no duplicate lfdis
        assert len(result) == 0
        contents = await cert_crud.select_all_certificates(session, 0, 5000)

        assert [(1, "SOMELFDI1"), (2, "SOMELFDI2")] == [(c.certificate_id, c.lfdi) for c in contents]
