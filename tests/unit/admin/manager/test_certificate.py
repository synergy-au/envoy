import datetime as dt

import pytest
import pytest_mock
import psycopg
import sqlalchemy as sa
from assertical.fixtures import postgres as pg_fixtures
from envoy_schema.admin.schema.certificate import CertificateAssignmentRequest

from envoy.admin.manager.certificate import CertificateManager
from envoy.server.model.aggregator import AggregatorCertificateAssignment
from envoy.admin import crud


@pytest.mark.anyio
async def test_add_many_certficates_for_aggregator_existing_cert_mocked(
    pg_base_config: psycopg.Connection, mocker: pytest_mock.MockerFixture
) -> None:
    """Testing to make sure all calls expected are being made for an existing certificate"""
    async with pg_fixtures.generate_async_session(pg_base_config) as session:
        crud_mock = mocker.patch("envoy.admin.crud.aggregator.assign_many_certificates")

        certs: list[CertificateAssignmentRequest] = [
            CertificateAssignmentRequest(certificate_id=4),
            CertificateAssignmentRequest(lfdi="SOMEFAKELFDI", expiry=dt.datetime.now() + dt.timedelta(365)),
        ]
        await CertificateManager.add_many_certificates_for_aggregator(session, 1, certs)
        crud_mock.assert_called_once()

        args, _ = crud_mock.call_args
        assert args[1] == 1, "Aggregator ID doesn't match"

        # Make sure the expected new assignment certificate IDs are being passed in
        assert [a for a in args[2]] == [7, 4], "Certificate IDs doesn't match"


@pytest.mark.anyio
async def test_add_many_certficates_for_aggregator_existing_cert(pg_base_config: psycopg.Connection) -> None:
    """Testing to make sure all calls expected are being made for an existing certificate"""
    async with pg_fixtures.generate_async_session(pg_base_config) as session:
        prior_assigns_q = await session.execute(sa.select(AggregatorCertificateAssignment))
        prior_assigns = prior_assigns_q.scalars().all()

        certs: list[CertificateAssignmentRequest] = [
            CertificateAssignmentRequest(certificate_id=4),
            CertificateAssignmentRequest(lfdi="SOMEFAKELFDI", expiry=dt.datetime.now() + dt.timedelta(365)),
        ]
        await CertificateManager.add_many_certificates_for_aggregator(session, 1, certs)

        # ensure new entry in table
        after_assigns_q = await session.execute(sa.select(AggregatorCertificateAssignment))
        after_assigns = after_assigns_q.scalars().all()
        assert len(prior_assigns) < len(after_assigns)

        all_certs = await crud.certificate.select_all_certificates(session, 0, 500)
        assert "SOMEFAKELFDI" in [ac.lfdi for ac in all_certs]
