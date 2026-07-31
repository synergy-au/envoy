import pytest
from assertical.asserts.type import assert_set_type
from assertical.fixtures.postgres import generate_async_session

from envoy.server.crud.site_group import fetch_site_group_membership


@pytest.mark.parametrize(
    "agg_id, site_id, expected",
    [
        (1, 1, {1, 2}),
        (None, 1, {1, 2}),
        (1, 2, {1, 4}),
        (None, 2, {1, 4}),
        (2, 3, {1, 5}),
        (None, 3, {1, 5}),
        (1, 4, set()),
        (None, 4, set()),
        (2, 1, None),  # Wrong agg_id
        (99, 1, None),  # Wrong agg_id
        (1, 99, None),  # Wrong site_id
        (99, 99, None),  # Wrong agg/site_id
        (1, 3, None),  # Wrong agg/site_id
    ],
)
@pytest.mark.anyio
async def test_fetch_site_group_membership(pg_base_config, agg_id: int | None, site_id: int, expected: set[int] | None):
    async with generate_async_session(pg_base_config) as session:
        result = await fetch_site_group_membership(session, agg_id, site_id)

    if expected is None:
        assert result is None
    else:
        assert_set_type(int, result, count=len(expected))
        assert result == expected
