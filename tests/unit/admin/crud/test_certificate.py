import datetime as dt

import pytest
import psycopg

from assertical.fixtures import postgres as pg_fixtures
from envoy.admin import crud
from envoy.server.model import base


@pytest.mark.anyio
async def test_count_certificates_for_aggregator(pg_base_config: psycopg.Connection) -> None:
    async with pg_fixtures.generate_async_session(pg_base_config) as session:
        assert (await crud.certificate.count_certificates_for_aggregator(session, aggregator_id=1)) == 3


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
        certs = await crud.certificate.select_all_certificates_for_aggregator(session, agg_id, 0, 100)
        assert all([isinstance(c, base.Certificate) for c in certs])
        assert expected_certs == [c.certificate_id for c in certs]


@pytest.mark.anyio
async def test_select_certificate(pg_base_config: psycopg.Connection) -> None:
    async with pg_fixtures.generate_async_session(pg_base_config) as session:

        cert_1 = await crud.certificate.select_certificate(session, 1)
        assert isinstance(cert_1, base.Certificate)
        assert cert_1.lfdi == "854d10a201ca99e5e90d3c3e1f9bc1c3bd075f3b"
        assert cert_1.expiry == dt.datetime.fromisoformat("2037-01-01T01:02:03+00")

        cert_2 = await crud.certificate.select_certificate(session, 2)
        assert isinstance(cert_2, base.Certificate)
        assert cert_2.lfdi == "403ba02aa36fa072c47eb3299daaafe94399adad"
        assert cert_2.expiry == dt.datetime.fromisoformat("2037-01-01T02:03:04+00")

        assert (await crud.certificate.select_certificate(session, 6)) is None
        assert (await crud.certificate.select_certificate(session, -1)) is None
