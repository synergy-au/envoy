import pytest
from assertical.asserts.type import assert_set_type
from assertical.fixtures.postgres import generate_async_session

from envoy.admin.crud.site_group import fetch_site_group_id_restrictions


@pytest.mark.parametrize(
    "site_id, group_name, expected",
    [
        (None, None, None),
        (1, None, {1, 2}),
        (2, None, {1, 4}),
        (3, None, {1, 5}),
        (99, None, set()),
        (None, "Group-1", {1}),
        (None, "Group-2", {2}),
        (2, "Group-2", set()),
        (1, "Group-2", {2}),
    ],
)
@pytest.mark.anyio
async def test_fetch_site_group_id_restrictions(
    pg_base_config, site_id: int | None, group_name: str | None, expected: set[int] | None
):

    async with generate_async_session(pg_base_config) as session:
        actual = await fetch_site_group_id_restrictions(session, site_id, group_name)
        if expected is None:
            assert actual is None
        else:
            assert_set_type(int, actual, len(expected))
            assert actual == expected
