import psycopg
import pytest
from assertical.fixtures import postgres

from envoy.admin import manager
from envoy.admin import crud
from envoy.server import exception


@pytest.mark.parametrize(
    "agg_id,cert_id,expected_ids",
    [
        (1, 1, [2, 3]),
        (1, 2, [1, 3]),
        (1, 3, [1, 2]),
    ],
)
@pytest.mark.anyio
async def test_unassign_certificate_for_aggregator(
    pg_base_config: psycopg.Connection, agg_id: int, cert_id: int, expected_ids: list[int]
) -> None:
    """Happy path tests for unassign_certificate_for_aggregator() method"""
    async with postgres.generate_async_session(pg_base_config) as session:
        # invoke method
        await manager.certificate.CertificateManager.unassign_certificate_for_aggregator(session, agg_id, cert_id)

        # query certificates
        actual_certs = await crud.certificate.select_all_certificates_for_aggregator(session, agg_id, 0, 500)

        # assert
        assert [a.certificate_id for a in actual_certs] == expected_ids


@pytest.mark.anyio
async def test_unassign_certificate_invalid_aggregator_id(pg_base_config: psycopg.Connection) -> None:
    """Confirm correct error raised"""
    async with postgres.generate_async_session(pg_base_config) as session:
        with pytest.raises(exception.NotFoundError, match="Aggregator with id 1111 not found"):
            await manager.certificate.CertificateManager.unassign_certificate_for_aggregator(session, 1111, 1)


@pytest.mark.anyio
async def test_unassign_certificate_invalid_certificate_id(pg_base_config: psycopg.Connection) -> None:
    """Confirm correct error raised"""
    async with postgres.generate_async_session(pg_base_config) as session:
        with pytest.raises(exception.NotFoundError, match="Certificate with id 1111 not found"):
            await manager.certificate.CertificateManager.unassign_certificate_for_aggregator(session, 1, 1111)
