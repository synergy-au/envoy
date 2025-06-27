import psycopg
import pytest
from assertical.fixtures.postgres import generate_async_session
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from envoy.admin.crud.aggregator import count_all_aggregators, select_all_aggregators, assign_many_certificates
from envoy.admin.crud.certificate import select_all_certificates_for_aggregator
from envoy.server.model.aggregator import Aggregator, AggregatorDomain, AggregatorCertificateAssignment


@pytest.mark.anyio
async def test_count_all_aggregators(pg_base_config):
    async with generate_async_session(pg_base_config) as session:
        assert (await count_all_aggregators(session)) == 3


@pytest.mark.anyio
async def test_count_all_aggregators_empty(pg_empty_config):
    async with generate_async_session(pg_empty_config) as session:
        assert (await count_all_aggregators(session)) == 0


@pytest.mark.parametrize(
    "start, limit, expected_aggregator_ids, expected_domain_ids",
    [
        (0, 500, [1, 2, 3], [1, 2, 3, 4]),
        (0, 1, [1], [1, 4]),
        (1, 1, [2], [2]),
        (2, 1, [3], [3]),
        (3, 1, [], []),
    ],
)
@pytest.mark.anyio
async def test_select_aggregators(
    pg_base_config, start: int, limit: int, expected_aggregator_ids: list[int], expected_domain_ids: list[int]
):
    async with generate_async_session(pg_base_config) as session:
        aggs = await select_all_aggregators(session, start, limit)
        assert len(aggs) == len(expected_aggregator_ids)
        assert all([isinstance(s, Aggregator) for s in aggs])
        assert expected_aggregator_ids == [a.aggregator_id for a in aggs]
        assert expected_domain_ids == sorted([d.aggregator_domain_id for a in aggs for d in a.domains])


@pytest.mark.parametrize("agg_id_to_delete", [1, 2, 3])
@pytest.mark.anyio
async def test_select_aggregators_no_domains(pg_base_config, agg_id_to_delete: int):

    async with generate_async_session(pg_base_config) as session:
        stmt = select(AggregatorDomain).where(AggregatorDomain.aggregator_id == agg_id_to_delete)
        resp = await session.execute(stmt)

        for domain_to_delete in resp.scalars().all():
            await session.delete(domain_to_delete)
        await session.commit()

    async with generate_async_session(pg_base_config) as session:
        aggs = await select_all_aggregators(session, 0, 99)
        agg_no_domains = [a for a in aggs if a.aggregator_id == agg_id_to_delete][0]
        assert len(agg_no_domains.domains) == 0


@pytest.mark.anyio
async def test_assign_many_certificates(pg_base_config: psycopg.Connection) -> None:
    async with generate_async_session(pg_base_config) as session:
        prior_assigns_q = await session.execute(select(AggregatorCertificateAssignment))
        prior_assigns = prior_assigns_q.scalars().all()
        await assign_many_certificates(session, 1, range(4, 6))

        # ensure certificates assigned
        resp = await select_all_certificates_for_aggregator(session, 1, 0, 5000)
        assert [c.certificate_id for c in resp] == [1, 2, 3, 4, 5]

        # ensure new entry in table
        after_assigns_q = await session.execute(select(AggregatorCertificateAssignment))
        after_assigns = after_assigns_q.scalars().all()
        assert len(prior_assigns) < len(after_assigns)


@pytest.mark.anyio
async def test_assign_many_certificates_already_assigned(pg_base_config: psycopg.Connection) -> None:
    """Should fail when an existing relationship is attempted to be created"""
    async with generate_async_session(pg_base_config) as session:
        with pytest.raises(IntegrityError):
            await assign_many_certificates(session, 1, [3])
