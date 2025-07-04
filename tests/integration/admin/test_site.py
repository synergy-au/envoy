import json
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Optional
from urllib.parse import quote_plus

import pytest
from assertical.asserts.time import assert_nowish
from assertical.fixtures.postgres import generate_async_session
from envoy_schema.admin.schema.site import SitePageResponse, SiteResponse, SiteUpdateRequest
from envoy_schema.admin.schema.site_group import SiteGroupPageResponse, SiteGroupResponse
from envoy_schema.admin.schema.uri import SiteGroupListUri, SiteGroupUri, SiteListUri, SiteUri
from envoy_schema.server.schema.sep2.types import DeviceCategory
from httpx import AsyncClient
from sqlalchemy import func, select

from envoy.admin.crud.site import count_all_site_groups, count_all_sites
from envoy.server.model.archive.doe import ArchiveDynamicOperatingEnvelope
from envoy.server.model.archive.site import ArchiveSite
from envoy.server.model.archive.tariff import ArchiveTariffGeneratedRate
from envoy.server.model.site import Site
from tests.integration.response import read_response_body_string


def _build_query_string(
    start: Optional[int], limit: Optional[int], group_filter: Optional[str], after: Optional[datetime]
) -> str:
    query = "?"
    if start is not None:
        query = query + f"&start={start}"
    if limit is not None:
        query = query + f"&limit={limit}"
    if group_filter is not None:
        query = query + f"&group={group_filter}"
    if after is not None:
        query = query + f"&after={quote_plus(after.isoformat())}"
    return query


SITE_1_DER_CFG_CHANGED_TIME = datetime(2022, 2, 9, 11, 6, 44, 500000, tzinfo=timezone.utc)  # This is from DERSetting
SITE_1_DER_AVAIL_CHANGED_TIME = datetime(2022, 7, 23, 10, 3, 23, 500000, tzinfo=timezone.utc)  # This is from DERAvail
SITE_1_DER_STATUS_CHANGED_TIME = datetime(2022, 11, 1, 11, 5, 4, 500000, tzinfo=timezone.utc)  # This is from DERStatus

SITE_1_DER_EXPECTED = (SITE_1_DER_CFG_CHANGED_TIME, SITE_1_DER_AVAIL_CHANGED_TIME, SITE_1_DER_STATUS_CHANGED_TIME)
SITE_X_NO_DER_EXPECTED = (None, None, None)


@pytest.mark.parametrize(
    "start, limit, group, after, expected_site_ids, expected_der_changed_times",
    [
        (
            None,
            None,
            None,
            None,
            [1, 2, 3, 4, 5, 6],
            [
                SITE_1_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
            ],
        ),
        (None, None, "Group-1", None, [1, 2, 3], [SITE_1_DER_EXPECTED, SITE_X_NO_DER_EXPECTED, SITE_X_NO_DER_EXPECTED]),
        (
            None,
            None,
            "Group-1",
            datetime(2022, 2, 3, 5, 6, 7, tzinfo=timezone.utc),
            [2, 3],
            [SITE_X_NO_DER_EXPECTED, SITE_X_NO_DER_EXPECTED],
        ),
        (None, None, "Group-2", None, [1], [SITE_1_DER_EXPECTED]),
        (None, None, "Group-3", None, [], []),
        (None, None, "Group-DNE", None, [], []),
        (
            0,
            10,
            None,
            None,
            [1, 2, 3, 4, 5, 6],
            [
                SITE_1_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
            ],
        ),
        (
            2,
            10,
            None,
            None,
            [3, 4, 5, 6],
            [SITE_X_NO_DER_EXPECTED, SITE_X_NO_DER_EXPECTED, SITE_X_NO_DER_EXPECTED, SITE_X_NO_DER_EXPECTED],
        ),
        (None, 2, None, None, [1, 2], [SITE_1_DER_EXPECTED, SITE_X_NO_DER_EXPECTED]),
        (2, 2, None, None, [3, 4], [SITE_X_NO_DER_EXPECTED, SITE_X_NO_DER_EXPECTED]),
        (5, 2, None, None, [6], [SITE_X_NO_DER_EXPECTED]),
        (1, 1, "Group-1", None, [2], [SITE_X_NO_DER_EXPECTED]),
        (
            None,
            None,
            None,
            datetime(2022, 2, 3, 5, 6, 7, tzinfo=timezone.utc),
            [2, 3, 4, 5, 6],
            [
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
                SITE_X_NO_DER_EXPECTED,
            ],
        ),
        (
            0,
            2,
            None,
            datetime(2022, 2, 3, 11, 12, 0, tzinfo=timezone.utc),
            [4, 5],
            [SITE_X_NO_DER_EXPECTED, SITE_X_NO_DER_EXPECTED],
        ),
        (3, 2, None, datetime(2022, 2, 3, 11, 12, 0, tzinfo=timezone.utc), [], []),
    ],
)
@pytest.mark.anyio
async def test_get_all_sites(
    admin_client_auth: AsyncClient,
    pg_base_config,
    start: int,
    limit: int,
    group: Optional[str],
    after: Optional[datetime],
    expected_site_ids: list[int],
    expected_der_changed_times: list[tuple[Optional[datetime], Optional[datetime], Optional[datetime]]],
):
    """expected_der_changed_times is the combination of the changed_time properties from
    (der_config, der_availability, der_status) that correspond 1-1 with the sites with expected_site_ids.
    It's there to validate the DER metadata being correctly assigned"""
    assert len(expected_site_ids) == len(
        expected_der_changed_times
    ), "There should be a 1-1 correspondence or this test is invalid"

    expected_total_sites: int
    async with generate_async_session(pg_base_config) as session:
        expected_total_sites = await count_all_sites(session, group, after)

    response = await admin_client_auth.get(SiteListUri + _build_query_string(start, limit, group, after))
    assert response.status_code == HTTPStatus.OK

    body = read_response_body_string(response)
    assert len(body) > 0
    site_page: SitePageResponse = SitePageResponse(**json.loads(body))

    assert isinstance(site_page.limit, int)
    assert isinstance(site_page.total_count, int)
    assert isinstance(site_page.start, int)
    assert len(site_page.sites) == len(expected_site_ids)
    assert all([isinstance(s, SiteResponse) for s in site_page.sites])

    assert (
        site_page.total_count == expected_total_sites
    ), f"There are only {expected_total_sites} sites available in the current config for group {group}"
    if limit is not None:
        assert site_page.limit == limit
    if start is not None:
        assert site_page.start == start

    assert [s.site_id for s in site_page.sites] == expected_site_ids

    assert [
        (
            s.der_config.changed_time if s.der_config else None,
            s.der_availability.changed_time if s.der_availability else None,
            s.der_status.changed_time if s.der_status else None,
        )
        for s in site_page.sites
    ] == expected_der_changed_times
    assert all((s.created_time == datetime(2000, 1, 1, tzinfo=timezone.utc) for s in site_page.sites))


@pytest.mark.parametrize(
    "start, limit, expected_group_count",
    [
        (None, None, [(1, 3), (2, 1), (3, 0)]),
        (None, 10, [(1, 3), (2, 1), (3, 0)]),
        (None, 2, [(1, 3), (2, 1)]),
        (1, 2, [(2, 1), (3, 0)]),
        (2, 2, [(3, 0)]),
        (3, 2, []),
        (3, None, []),
    ],
)
@pytest.mark.anyio
async def test_get_all_site_groups(
    admin_client_auth: AsyncClient,
    pg_base_config,
    start: int,
    limit: int,
    expected_group_count: list[tuple[int, int]],
):
    expected_total_groups: int
    async with generate_async_session(pg_base_config) as session:
        expected_total_groups = await count_all_site_groups(session)

    response = await admin_client_auth.get(SiteGroupListUri + _build_query_string(start, limit, None, None))
    assert response.status_code == HTTPStatus.OK

    body = read_response_body_string(response)
    assert len(body) > 0
    group_page: SiteGroupPageResponse = SiteGroupPageResponse(**json.loads(body))

    assert isinstance(group_page.limit, int)
    assert isinstance(group_page.total_count, int)
    assert isinstance(group_page.start, int)
    assert len(group_page.groups) == len(expected_group_count)
    assert all([isinstance(s, SiteGroupResponse) for s in group_page.groups])

    assert (
        group_page.total_count == expected_total_groups
    ), f"There are only {expected_total_groups} sites available in the current config"
    if limit is not None:
        assert group_page.limit == limit
    if start is not None:
        assert group_page.start == start

    assert [(g.site_group_id, g.total_sites) for g in group_page.groups] == expected_group_count


@pytest.mark.parametrize(
    "group_name, expected_group_count",
    [
        ("Group-1", (1, 3)),
        ("Group-2", (2, 1)),
        ("Group-3", (3, 0)),
        ("Group-4", None),
    ],
)
@pytest.mark.anyio
async def test_get_site_groups(
    admin_client_auth: AsyncClient,
    group_name: str,
    expected_group_count: Optional[tuple[int, int]],
):

    response = await admin_client_auth.get(SiteGroupUri.format(group_name=group_name))

    if expected_group_count is None:
        assert response.status_code == HTTPStatus.NOT_FOUND
    else:
        assert response.status_code == HTTPStatus.OK

        body = read_response_body_string(response)
        assert len(body) > 0
        group: SiteGroupResponse = SiteGroupResponse(**json.loads(body))

        assert isinstance(group, SiteGroupResponse)
        assert group.site_group_id == expected_group_count[0]
        assert group.total_sites == expected_group_count[1]
        assert group.name == group_name


@pytest.mark.parametrize(
    "site_id, expected_der_changed_time",
    [
        (1, SITE_1_DER_EXPECTED),
        (2, SITE_X_NO_DER_EXPECTED),
        (3, SITE_X_NO_DER_EXPECTED),
        (99, None),
    ],
)
@pytest.mark.anyio
async def test_get_site(
    admin_client_auth: AsyncClient,
    site_id: int,
    expected_der_changed_time: Optional[tuple[Optional[datetime], Optional[datetime], Optional[datetime]]],
):
    """expected_der_changed_times is the combination of the changed_time properties from
    (der_config, der_availability, der_status) It's there to validate the DER metadata being correctly assigned"""

    response = await admin_client_auth.get(SiteUri.format(site_id=site_id))

    if expected_der_changed_time is None:
        assert response.status_code == HTTPStatus.NOT_FOUND
    else:
        body = read_response_body_string(response)
        assert len(body) > 0
        site_response: SiteResponse = SiteResponse(**json.loads(body))

        assert site_response.site_id == site_id
        assert (
            site_response.der_config.changed_time if site_response.der_config else None,
            site_response.der_availability.changed_time if site_response.der_availability else None,
            site_response.der_status.changed_time if site_response.der_status else None,
        ) == expected_der_changed_time


@pytest.mark.parametrize(
    "site_id, expected_status, archive_site_count, archive_doe_count, archive_price_count",
    [
        (1, HTTPStatus.NO_CONTENT, 1, 3, 3),
        (2, HTTPStatus.NO_CONTENT, 1, 1, 1),
        (3, HTTPStatus.NO_CONTENT, 1, 0, 0),
        (4, HTTPStatus.NO_CONTENT, 1, 0, 0),
        (5, HTTPStatus.NO_CONTENT, 1, 0, 0),
        (99, HTTPStatus.NOT_FOUND, 0, 0, 0),
    ],
)
@pytest.mark.anyio
async def test_delete_site_archives(
    admin_client_auth: AsyncClient,
    pg_base_config,
    site_id: int,
    expected_status: HTTPStatus,
    archive_site_count: int,
    archive_doe_count: int,
    archive_price_count: int,
):
    """Tests that deleting sites generates archive records"""

    response = await admin_client_auth.delete(SiteUri.format(site_id=site_id))
    assert response.status_code == expected_status

    # Count archive rows are generated
    async with generate_async_session(pg_base_config) as session:
        actual_archive_site_count = (
            await session.execute(
                select(func.count()).select_from(ArchiveSite).where(ArchiveSite.deleted_time.is_not(None))
            )
        ).scalar_one()
        actual_archive_doe_count = (
            await session.execute(
                select(func.count())
                .select_from(ArchiveDynamicOperatingEnvelope)
                .where(ArchiveDynamicOperatingEnvelope.deleted_time.is_not(None))
            )
        ).scalar_one()
        actual_archive_price_count = (
            await session.execute(
                select(func.count())
                .select_from(ArchiveTariffGeneratedRate)
                .where(ArchiveTariffGeneratedRate.deleted_time.is_not(None))
            )
        ).scalar_one()
        assert actual_archive_site_count == archive_site_count
        assert actual_archive_doe_count == archive_doe_count
        assert actual_archive_price_count == archive_price_count

    # Subsequent query will now 404
    response = await admin_client_auth.delete(SiteUri.format(site_id=site_id))
    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.parametrize(
    "site_id, update_request, expected_status, expected_nmi, expected_tz, expected_device_category, expected_post_rate",
    [
        (
            1,
            SiteUpdateRequest(nmi="", timezone_id="Australia/Perth", device_category=None, post_rate_seconds=0),
            HTTPStatus.NO_CONTENT,
            None,
            "Australia/Perth",
            0,
            None,
        ),
        (
            1,
            SiteUpdateRequest(nmi="abc456", timezone_id=None, device_category=None, post_rate_seconds=None),
            HTTPStatus.NO_CONTENT,
            "abc456",
            "Australia/Brisbane",
            0,
            111,
        ),
        (
            2,
            SiteUpdateRequest(
                nmi="abc123", timezone_id=None, device_category=DeviceCategory.FUEL_CELL, post_rate_seconds=23
            ),
            HTTPStatus.NO_CONTENT,
            "abc123",
            "Australia/Brisbane",
            DeviceCategory.FUEL_CELL,
            23,
        ),
        (
            2,
            SiteUpdateRequest(nmi="", timezone_id=None, device_category=None, post_rate_seconds=None),
            HTTPStatus.NO_CONTENT,
            None,
            "Australia/Brisbane",
            1,
            None,
        ),
        (
            5,
            SiteUpdateRequest(
                nmi=None,
                timezone_id="Australia/Sydney",
                device_category=DeviceCategory.ELECTRIC_VEHICLE | DeviceCategory.HOT_TUB,
                post_rate_seconds=-1,
            ),
            HTTPStatus.NO_CONTENT,
            "5555555555",
            "Australia/Sydney",
            DeviceCategory.ELECTRIC_VEHICLE | DeviceCategory.HOT_TUB,
            None,
        ),
        (
            99,
            SiteUpdateRequest(nmi="", timezone_id=None, device_category=None, post_rate_seconds=456),
            HTTPStatus.NOT_FOUND,
            None,
            None,
            None,
            None,
        ),
    ],
)
@pytest.mark.anyio
async def test_update_site_archives(
    admin_client_auth: AsyncClient,
    pg_base_config,
    site_id: int,
    update_request: SiteUpdateRequest,
    expected_status: HTTPStatus,
    expected_nmi: Optional[str],
    expected_tz: Optional[str],
    expected_device_category: Optional[DeviceCategory],
    expected_post_rate: Optional[int],
):
    """Tests that updating sites generates archive records"""

    response = await admin_client_auth.post(SiteUri.format(site_id=site_id), content=update_request.model_dump_json())
    assert response.status_code == expected_status

    if response.status_code == HTTPStatus.NOT_FOUND:
        expected_archive_count = 0
        check_site = False
    else:
        expected_archive_count = 1
        check_site = True

    # Count archive rows are generated, check the row updated
    async with generate_async_session(pg_base_config) as session:
        actual_archive_site_count = (
            await session.execute(
                select(func.count()).select_from(ArchiveSite).where(ArchiveSite.deleted_time.is_(None))
            )
        ).scalar_one()
        assert actual_archive_site_count == expected_archive_count

        if check_site:
            actual_site = (await session.execute(select(Site).where(Site.site_id == site_id))).scalar_one()
            assert actual_site.nmi == expected_nmi
            assert actual_site.post_rate_seconds == expected_post_rate
            assert actual_site.device_category == expected_device_category
            assert actual_site.timezone_id == expected_tz
            assert_nowish(actual_site.changed_time)
