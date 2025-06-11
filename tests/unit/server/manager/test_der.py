import unittest.mock as mock
from datetime import datetime, timedelta, timezone

import pytest
from assertical.asserts.generator import assert_class_instance_equality
from assertical.asserts.time import assert_datetime_equal, assert_nowish
from assertical.fake.asyncio import create_async_result
from assertical.fake.generator import clone_class_instance, generate_class_instance
from assertical.fake.sqlalchemy import assert_mock_session, create_mock_session
from assertical.fixtures.postgres import generate_async_session
from envoy_schema.server.schema.sep2.der import (
    AlarmStatusType,
    ConnectStatusType,
    DERAvailability,
    DERCapability,
    DERControlType,
    DERListResponse,
    DERSettings,
    DERStatus,
    DOESupportedMode,
)
from sqlalchemy import func, select

from envoy.server.exception import NotFoundError
from envoy.server.manager.der import (
    DERAvailabilityManager,
    DERCapabilityManager,
    DERManager,
    DERSettingsManager,
    DERStatusManager,
    site_der_for_site,
)
from envoy.server.manager.der_constants import PUBLIC_SITE_DER_ID
from envoy.server.mapper.sep2.der import to_hex_binary
from envoy.server.model.archive.site import (
    ArchiveSiteDERAvailability,
    ArchiveSiteDERRating,
    ArchiveSiteDERSetting,
    ArchiveSiteDERStatus,
)
from envoy.server.model.config.server import RuntimeServerConfig
from envoy.server.model.site import Site, SiteDER, SiteDERAvailability, SiteDERRating, SiteDERSetting, SiteDERStatus
from envoy.server.model.subscription import SubscriptionResource
from envoy.server.request_scope import SiteRequestScope


@mock.patch("envoy.server.manager.der.select_site_der_for_site")
@mock.patch("envoy.server.manager.der.select_single_site_with_site_id")
@mock.patch("envoy.server.manager.der.generate_default_site_der")
@pytest.mark.anyio
async def test_site_der_for_site_no_der(
    mock_generate_default_site_der: mock.MagicMock,
    mock_select_single_site_with_site_id: mock.MagicMock,
    mock_select_site_der_for_site: mock.MagicMock,
):
    """Fetch when no existing SiteDER exists"""
    site_id = 123
    agg_id = 456
    mock_session = create_mock_session()

    site_der: SiteDER = generate_class_instance(SiteDER)
    site: Site = generate_class_instance(Site)
    mock_select_site_der_for_site.return_value = None
    mock_select_single_site_with_site_id.return_value = site
    mock_generate_default_site_der.return_value = site_der

    result = await site_der_for_site(mock_session, agg_id, site_id)
    assert result is site_der

    mock_select_site_der_for_site.assert_called_once_with(mock_session, site_id=site_id, aggregator_id=agg_id)
    mock_select_single_site_with_site_id.assert_called_with(mock_session, site_id=site_id, aggregator_id=agg_id)
    mock_generate_default_site_der.assert_called_with(site_id=site_id, changed_time=site.changed_time)
    assert_mock_session(mock_session)


@mock.patch("envoy.server.manager.der.select_site_der_for_site")
@mock.patch("envoy.server.manager.der.select_single_site_with_site_id")
@mock.patch("envoy.server.manager.der.generate_default_site_der")
@pytest.mark.anyio
async def test_site_der_for_site_existing_der(
    mock_generate_default_site_der: mock.MagicMock,
    mock_select_single_site_with_site_id: mock.MagicMock,
    mock_select_site_der_for_site: mock.MagicMock,
):
    """Fetch when SiteDER already exists"""
    site_id = 123
    agg_id = 456
    mock_session = create_mock_session()

    site_der: SiteDER = generate_class_instance(SiteDER)
    mock_select_site_der_for_site.return_value = site_der

    result = await site_der_for_site(mock_session, agg_id, site_id)
    assert result is site_der

    mock_select_site_der_for_site.assert_called_once_with(mock_session, site_id=site_id, aggregator_id=agg_id)
    mock_select_single_site_with_site_id.assert_not_called()
    mock_generate_default_site_der.assert_not_called()
    assert_mock_session(mock_session)


@mock.patch("envoy.server.manager.der.select_site_der_for_site")
@mock.patch("envoy.server.manager.der.select_single_site_with_site_id")
@mock.patch("envoy.server.manager.der.generate_default_site_der")
@pytest.mark.anyio
async def test_site_der_for_site_inaccessible_site(
    mock_generate_default_site_der: mock.MagicMock,
    mock_select_single_site_with_site_id: mock.MagicMock,
    mock_select_site_der_for_site: mock.MagicMock,
):
    """Fetch when SiteDER doesn't exist and the site isn't accessible"""
    site_id = 123
    agg_id = 456
    mock_session = create_mock_session()

    mock_select_site_der_for_site.return_value = None
    mock_select_single_site_with_site_id.return_value = None

    with pytest.raises(NotFoundError):
        await site_der_for_site(mock_session, agg_id, site_id)

    mock_select_site_der_for_site.assert_called_once_with(mock_session, site_id=site_id, aggregator_id=agg_id)
    mock_select_single_site_with_site_id.assert_called_with(mock_session, site_id=site_id, aggregator_id=agg_id)
    mock_generate_default_site_der.assert_not_called()
    assert_mock_session(mock_session)


@mock.patch("envoy.server.manager.der.DERMapper")
@mock.patch("envoy.server.manager.der.site_der_for_site")
@pytest.mark.anyio
async def test_fetch_der_for_site_der_exists(
    mock_site_der_for_site: mock.MagicMock,
    mock_DERMapper: mock.MagicMock,
):
    """Fetch when site_der_for_site returns an instance"""
    scope: SiteRequestScope = generate_class_instance(SiteRequestScope, seed=1001)
    mock_session = create_mock_session()

    site_der: SiteDER = generate_class_instance(SiteDER, seed=101)
    mock_site_der_for_site.return_value = site_der
    mock_map = mock.Mock()
    mock_DERMapper.map_to_response = mock.Mock(return_value=mock_map)

    result = await DERManager.fetch_der_for_site(mock_session, scope, PUBLIC_SITE_DER_ID)
    assert result is mock_map

    assert site_der.site_der_id == PUBLIC_SITE_DER_ID, "This should've been set during the fetch"
    mock_DERMapper.map_to_response.assert_called_once_with(scope, site_der, None)
    mock_site_der_for_site.assert_called_once_with(
        mock_session, aggregator_id=scope.aggregator_id, site_id=scope.site_id
    )
    assert_mock_session(mock_session)


@mock.patch("envoy.server.manager.der.DERMapper")
@mock.patch("envoy.server.manager.der.site_der_for_site")
@pytest.mark.anyio
async def test_fetch_der_for_site_bad_der_id(
    mock_site_der_for_site: mock.MagicMock,
    mock_DERMapper: mock.MagicMock,
):
    """Fetch when DER ID is incorrect"""
    scope: SiteRequestScope = generate_class_instance(SiteRequestScope, seed=1001)
    mock_session = create_mock_session()

    with pytest.raises(NotFoundError):
        await DERManager.fetch_der_for_site(mock_session, scope, PUBLIC_SITE_DER_ID + 1)

    mock_DERMapper.assert_not_called()
    mock_site_der_for_site.assert_not_called()
    assert_mock_session(mock_session)


AFTER_EPOCH = datetime(2022, 10, 9, 8, 7, 6, tzinfo=timezone.utc)


@mock.patch("envoy.server.manager.der.site_der_for_site")
@mock.patch("envoy.server.manager.der.RuntimeServerConfigManager.fetch_current_config")
@pytest.mark.parametrize(
    "contains, start, limit, after, expected_count",
    [
        (True, 0, 99, AFTER_EPOCH - timedelta(seconds=10), 1),
        (False, 0, 99, AFTER_EPOCH - timedelta(seconds=10), 1),
        (True, 0, 0, AFTER_EPOCH - timedelta(seconds=10), 0),
        (False, 0, 0, AFTER_EPOCH - timedelta(seconds=10), 0),
        (True, 1, 99, AFTER_EPOCH - timedelta(seconds=10), 0),
        (True, 0, 99, AFTER_EPOCH + timedelta(seconds=10), 0),
    ],
)
@pytest.mark.anyio
async def test_fetch_der_list_for_site_pagination(
    mock_fetch_current_config: mock.MagicMock,
    mock_site_der_for_site: mock.MagicMock,
    start: int,
    limit: int,
    after: datetime,
    contains: bool,
    expected_count: int,
):
    """Fetch when site_der_for_site returns an instance"""
    scope: SiteRequestScope = generate_class_instance(SiteRequestScope, seed=1001)
    mock_session = create_mock_session()
    mock_session.__contains__ = mock.Mock(return_value=contains)
    mock_session.expunge = mock.Mock()

    site_der: SiteDER = generate_class_instance(SiteDER, seed=101)
    site_der.changed_time = AFTER_EPOCH
    mock_site_der_for_site.return_value = site_der

    config = RuntimeServerConfig()
    mock_fetch_current_config.return_value = config

    result = await DERManager.fetch_der_list_for_site(mock_session, scope, start, limit, after)
    assert isinstance(result, DERListResponse)

    assert len(result.DER_) == expected_count
    mock_site_der_for_site.assert_called_once_with(
        mock_session, aggregator_id=scope.aggregator_id, site_id=scope.site_id
    )
    assert_mock_session(mock_session)

    # We only expunge objects that exist in the session (otherwise we get errors)
    if contains:
        mock_session.expunge.assert_called_once_with(site_der)
    else:
        mock_session.expunge.assert_not_called()


@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
        (1, 2, PUBLIC_SITE_DER_ID),  # There is no entity record in the db
        (1, 4, PUBLIC_SITE_DER_ID),  # There is DER or entity record in the db
    ],
)
@pytest.mark.anyio
async def test_fetch_der_capability_not_found(pg_base_config, agg_id: int, site_id: int, der_id: int):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(SiteRequestScope, seed=1001)

    async with generate_async_session(pg_base_config) as session:
        with pytest.raises(NotFoundError):
            await DERCapabilityManager.fetch_der_capability_for_site(
                session,
                der_id,
                scope,
            )


@mock.patch("envoy.server.manager.der.NotificationManager")
@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
    ],
)
@pytest.mark.anyio
async def test_upsert_der_capability_not_found(
    mock_NotificationManager: mock.MagicMock, pg_base_config, agg_id: int, site_id: int, der_id: int
):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(SiteRequestScope, seed=1001)

    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    async with generate_async_session(pg_base_config) as session:
        initial_count = (await session.execute(select(func.count()).select_from(SiteDERRating))).scalar_one()

    async with generate_async_session(pg_base_config) as session:
        e: DERCapability = generate_class_instance(DERCapability, generate_relationships=True)
        e.modesSupported = to_hex_binary(DERControlType.OP_MOD_CONNECT)
        e.doeModesSupported = to_hex_binary(DOESupportedMode.OP_MOD_IMPORT_LIMIT_W)

        with pytest.raises(NotFoundError):
            await DERCapabilityManager.upsert_der_capability_for_site(
                session,
                der_id,
                scope,
                e,
            )

    # Validate we haven't added any rows on accident
    async with generate_async_session(pg_base_config) as session:
        resp = await session.execute(select(func.count()).select_from(SiteDERRating))
        assert resp.scalar_one() == initial_count

    mock_NotificationManager.notify_changed_deleted_entities.assert_not_called()


@mock.patch("envoy.server.manager.der.NotificationManager")
@mock.patch("envoy.server.manager.der.utc_now")
@pytest.mark.parametrize(
    "site_id",
    [
        1,  # This site has existing DER record and entity record (will update entity record)
        2,  # This site has existing DER record but no entity record (will insert entity record)
        4,  # This site has no DER/entity record (will insert both records)
    ],
)
@pytest.mark.anyio
async def test_upsert_der_capability_roundtrip(
    mock_utc_now: mock.MagicMock, mock_NotificationManager: mock.MagicMock, pg_base_config, site_id: int
):
    """Tests the various success paths through updating"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=1, site_id=site_id, href_prefix="/foo/bar"
    )
    now = datetime(2023, 5, 6, 7, 8, 11)

    mock_utc_now.return_value = now
    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    # Do the upsert
    expected: DERCapability = generate_class_instance(DERCapability, seed=5001, generate_relationships=True)
    expected.modesSupported = to_hex_binary(
        DERControlType.OP_MOD_HVRT_MUST_TRIP | DERControlType.OP_MOD_HVRT_MOMENTARY_CESSATION
    )
    expected.doeModesSupported = to_hex_binary(DOESupportedMode.OP_MOD_EXPORT_LIMIT_W)
    async with generate_async_session(pg_base_config) as session:
        await DERCapabilityManager.upsert_der_capability_for_site(
            session,
            PUBLIC_SITE_DER_ID,
            scope,
            clone_class_instance(expected),
        )

    # Use a new session to query everything back
    async with generate_async_session(pg_base_config) as session:
        actual = await DERCapabilityManager.fetch_der_capability_for_site(session, PUBLIC_SITE_DER_ID, scope)

        assert_class_instance_equality(
            DERCapability,
            expected,
            actual,
            ignored_properties=set(["href", "subscribable", "type"]),
        )
        assert actual.href.startswith(scope.href_prefix)
        assert str(site_id) in actual.href

    # Archive records will be written on updates, otherwise they won't be touched
    async with generate_async_session(pg_base_config) as session:
        archive_records = (await session.execute(select(ArchiveSiteDERRating))).scalars().all()
        if site_id == 1:
            # site_id 1 is the test case that will update an existing record
            assert len(archive_records) == 1
            assert archive_records[0].max_a_value == 106, "This is the original value from the DB"
            assert archive_records[0].deleted_time is None
            assert_nowish(archive_records[0].archive_time)
        else:
            assert len(archive_records) == 0

    mock_NotificationManager.notify_changed_deleted_entities.assert_called_once_with(
        SubscriptionResource.SITE_DER_RATING, now
    )


@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
        (1, 2, PUBLIC_SITE_DER_ID),  # There is no entity record in the db
        (1, 4, PUBLIC_SITE_DER_ID),  # There is DER or entity record in the db
    ],
)
@pytest.mark.anyio
async def test_fetch_der_settings_not_found(pg_base_config, agg_id: int, site_id: int, der_id: int):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=agg_id, site_id=site_id
    )

    async with generate_async_session(pg_base_config) as session:
        with pytest.raises(NotFoundError):
            await DERSettingsManager.fetch_der_settings_for_site(
                session,
                der_id,
                scope,
            )


@mock.patch("envoy.server.manager.der.NotificationManager")
@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
    ],
)
@pytest.mark.anyio
async def test_upsert_der_settings_not_found(
    mock_NotificationManager: mock.MagicMock, pg_base_config, agg_id: int, site_id: int, der_id: int
):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=agg_id, site_id=site_id
    )

    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    async with generate_async_session(pg_base_config) as session:
        initial_count = (await session.execute(select(func.count()).select_from(SiteDERSetting))).scalar_one()

    async with generate_async_session(pg_base_config) as session:
        e: DERSettings = generate_class_instance(DERSettings, generate_relationships=True)
        e.modesEnabled = to_hex_binary(DERControlType.OP_MOD_FIXED_PF_ABSORB_W)
        e.doeModesEnabled = to_hex_binary(DOESupportedMode.OP_MOD_IMPORT_LIMIT_W)

        with pytest.raises(NotFoundError):
            await DERSettingsManager.upsert_der_settings_for_site(
                session,
                der_id,
                scope,
                e,
            )

    # Validate we haven't added any rows on accident
    async with generate_async_session(pg_base_config) as session:
        resp = await session.execute(select(func.count()).select_from(SiteDERSetting))
        assert resp.scalar_one() == initial_count

    mock_NotificationManager.notify_changed_deleted_entities.assert_not_called()


@mock.patch("envoy.server.manager.der.NotificationManager")
@mock.patch("envoy.server.manager.der.utc_now")
@pytest.mark.parametrize(
    "site_id",
    [
        1,  # This site has existing DER record and entity record (will update entity record)
        2,  # This site has existing DER record but no entity record (will insert entity record)
        4,  # This site has no DER/entity record (will insert both records)
    ],
)
@pytest.mark.anyio
async def test_upsert_der_settings_roundtrip(
    mock_utc_now: mock.MagicMock, mock_NotificationManager: mock.MagicMock, pg_base_config, site_id: int
):
    """Tests the various success paths through updating"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=1, site_id=site_id, href_prefix="/foo/bar"
    )
    now = datetime(2023, 5, 2, 7, 8, 9)

    mock_utc_now.return_value = now
    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    # Do the upsert
    expected: DERSettings = generate_class_instance(DERSettings, seed=5001, generate_relationships=True)
    expected.modesEnabled = to_hex_binary(DERControlType.OP_MOD_MAX_LIM_W | DERControlType.CHARGE_MODE)
    expected.doeModesEnabled = to_hex_binary(DERControlType.OP_MOD_CONNECT)
    async with generate_async_session(pg_base_config) as session:
        await DERSettingsManager.upsert_der_settings_for_site(
            session,
            PUBLIC_SITE_DER_ID,
            scope,
            clone_class_instance(expected),
        )

    # Use a new session to query everything back
    async with generate_async_session(pg_base_config) as session:
        actual = await DERSettingsManager.fetch_der_settings_for_site(session, PUBLIC_SITE_DER_ID, scope)

        assert_class_instance_equality(
            DERSettings,
            expected,
            actual,
            ignored_properties=set(["href", "subscribable", "type", "updatedTime"]),
        )
        assert actual.href.startswith(scope.href_prefix)
        assert str(site_id) in actual.href
        assert_datetime_equal(now, actual.updatedTime)  # Should be set to server time

    # Archive records will be written on updates, otherwise they won't be touched
    async with generate_async_session(pg_base_config) as session:
        archive_records = (await session.execute(select(ArchiveSiteDERSetting))).scalars().all()
        if site_id == 1:
            # site_id 1 is the test case that will update an existing record
            assert len(archive_records) == 1
            assert archive_records[0].es_delay == 406, "This is the original value from the DB"
            assert archive_records[0].deleted_time is None
            assert_nowish(archive_records[0].archive_time)
        else:
            assert len(archive_records) == 0

    mock_NotificationManager.notify_changed_deleted_entities.assert_called_once_with(
        SubscriptionResource.SITE_DER_SETTING, now
    )


@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
        (1, 2, PUBLIC_SITE_DER_ID),  # There is no entity record in the db
        (1, 4, PUBLIC_SITE_DER_ID),  # There is DER or entity record in the db
    ],
)
@pytest.mark.anyio
async def test_fetch_der_availability_not_found(pg_base_config, agg_id: int, site_id: int, der_id: int):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=agg_id, site_id=site_id
    )

    async with generate_async_session(pg_base_config) as session:
        with pytest.raises(NotFoundError):
            await DERAvailabilityManager.fetch_der_availability_for_site(
                session,
                der_id,
                scope,
            )


@mock.patch("envoy.server.manager.der.NotificationManager")
@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
    ],
)
@pytest.mark.anyio
async def test_upsert_der_availability_not_found(
    mock_NotificationManager: mock.MagicMock, pg_base_config, agg_id: int, site_id: int, der_id: int
):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=agg_id, site_id=site_id
    )

    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    async with generate_async_session(pg_base_config) as session:
        initial_count = (await session.execute(select(func.count()).select_from(SiteDERAvailability))).scalar_one()

    async with generate_async_session(pg_base_config) as session:
        e: DERAvailability = generate_class_instance(DERAvailability, generate_relationships=True)

        with pytest.raises(NotFoundError):
            await DERAvailabilityManager.upsert_der_availability_for_site(
                session,
                der_id,
                scope,
                e,
            )

    # Validate we haven't added any rows on accident
    async with generate_async_session(pg_base_config) as session:
        resp = await session.execute(select(func.count()).select_from(SiteDERAvailability))
        assert resp.scalar_one() == initial_count

    mock_NotificationManager.notify_changed_deleted_entities.assert_not_called()


@mock.patch("envoy.server.manager.der.NotificationManager")
@mock.patch("envoy.server.manager.der.utc_now")
@pytest.mark.parametrize(
    "site_id",
    [
        1,  # This site has existing DER record and entity record (will update entity record)
        2,  # This site has existing DER record but no entity record (will insert entity record)
        4,  # This site has no DER/entity record (will insert both records)
    ],
)
@pytest.mark.anyio
async def test_upsert_der_availability_roundtrip(
    mock_utc_now: mock.MagicMock, mock_NotificationManager: mock.MagicMock, pg_base_config, site_id: int
):
    """Tests the various success paths through updating"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=1, site_id=site_id, href_prefix="/foo/bar"
    )
    now = datetime(2024, 5, 6, 7, 8, 9)

    mock_utc_now.return_value = now
    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    # Do the upsert
    expected: DERAvailability = generate_class_instance(DERAvailability, seed=5001, generate_relationships=True)
    async with generate_async_session(pg_base_config) as session:
        await DERAvailabilityManager.upsert_der_availability_for_site(
            session,
            PUBLIC_SITE_DER_ID,
            scope,
            clone_class_instance(expected),
        )

    # Use a new session to query everything back
    async with generate_async_session(pg_base_config) as session:
        actual = await DERAvailabilityManager.fetch_der_availability_for_site(session, PUBLIC_SITE_DER_ID, scope)

        assert_class_instance_equality(
            DERAvailability,
            expected,
            actual,
            ignored_properties=set(["href", "subscribable", "type", "readingTime"]),
        )
        assert actual.href.startswith(scope.href_prefix)
        assert str(site_id) in actual.href
        assert_datetime_equal(now, actual.readingTime)  # Should be set to server time

    # Archive records will be written on updates, otherwise they won't be touched
    async with generate_async_session(pg_base_config) as session:
        archive_records = (await session.execute(select(ArchiveSiteDERAvailability))).scalars().all()
        if site_id == 1:
            # site_id 1 is the test case that will update an existing record
            assert len(archive_records) == 1
            assert archive_records[0].availability_duration_sec == 202, "This is the original value from the DB"
            assert archive_records[0].deleted_time is None
            assert_nowish(archive_records[0].archive_time)
        else:
            assert len(archive_records) == 0

    mock_NotificationManager.notify_changed_deleted_entities.assert_called_once_with(
        SubscriptionResource.SITE_DER_AVAILABILITY, now
    )


@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
        (1, 2, PUBLIC_SITE_DER_ID),  # There is no entity record in the db
        (1, 4, PUBLIC_SITE_DER_ID),  # There is DER or entity record in the db
    ],
)
@pytest.mark.anyio
async def test_fetch_der_status_not_found(pg_base_config, agg_id: int, site_id: int, der_id: int):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=agg_id, site_id=site_id
    )

    async with generate_async_session(pg_base_config) as session:
        with pytest.raises(NotFoundError):
            await DERStatusManager.fetch_der_status_for_site(
                session,
                der_id,
                scope,
            )


@mock.patch("envoy.server.manager.der.NotificationManager")
@pytest.mark.parametrize(
    "agg_id, site_id, der_id",
    [
        (99, 1, PUBLIC_SITE_DER_ID),  # invalid agg_id
        (2, 1, PUBLIC_SITE_DER_ID),  # Invalid agg_id
        (1, 99, PUBLIC_SITE_DER_ID),  # Invalid site id
        (1, 1, PUBLIC_SITE_DER_ID + 1),  # invalid DER id
    ],
)
@pytest.mark.anyio
async def test_upsert_der_status_not_found(
    mock_NotificationManager: mock.MagicMock, pg_base_config, agg_id: int, site_id: int, der_id: int
):
    """Tests the various ways a NotFoundError can be raised"""
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=agg_id, site_id=site_id
    )

    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    async with generate_async_session(pg_base_config) as session:
        initial_count = (await session.execute(select(func.count()).select_from(SiteDERStatus))).scalar_one()

    async with generate_async_session(pg_base_config) as session:
        e: DERStatus = generate_class_instance(DERStatus, generate_relationships=True)
        e.alarmStatus = to_hex_binary(AlarmStatusType.DER_FAULT_EMERGENCY_REMOTE)
        e.genConnectStatus.value = to_hex_binary(ConnectStatusType.AVAILABLE | ConnectStatusType.OPERATING)
        e.storConnectStatus.value = to_hex_binary(ConnectStatusType.TEST | ConnectStatusType.FAULT_ERROR)
        e.manufacturerStatus.value = "ab-12$"  # Length limit on field
        with pytest.raises(NotFoundError):
            await DERStatusManager.upsert_der_status_for_site(
                session,
                der_id,
                scope,
                e,
            )

    # Validate we haven't added any rows on accident
    async with generate_async_session(pg_base_config) as session:
        resp = await session.execute(select(func.count()).select_from(SiteDERStatus))
        assert resp.scalar_one() == initial_count

    mock_NotificationManager.notify_changed_deleted_entities.assert_not_called()


@mock.patch("envoy.server.manager.der.NotificationManager")
@mock.patch("envoy.server.manager.der.utc_now")
@pytest.mark.parametrize(
    "site_id",
    [
        1,  # This site has existing DER record and entity record (will update entity record)
        2,  # This site has existing DER record but no entity record (will insert entity record)
        4,  # This site has no DER/entity record (will insert both records)
    ],
)
@pytest.mark.anyio
async def test_upsert_der_status_roundtrip(
    mock_utc_now: mock.MagicMock, mock_NotificationManager: mock.MagicMock, pg_base_config, site_id: int
):
    """Tests the various success paths through updating"""
    agg_id = 1
    scope: SiteRequestScope = generate_class_instance(
        SiteRequestScope, seed=1001, aggregator_id=agg_id, site_id=site_id, href_prefix="/foo/bar"
    )
    now = datetime(2023, 5, 6, 7, 8, 9)

    mock_utc_now.return_value = now
    mock_NotificationManager.notify_changed_deleted_entities = mock.Mock(return_value=create_async_result(True))

    # Do the upsert
    expected: DERStatus = generate_class_instance(DERStatus, seed=5001, generate_relationships=True)
    expected.alarmStatus = to_hex_binary(
        AlarmStatusType.DER_FAULT_OVER_FREQUENCY | AlarmStatusType.DER_FAULT_VOLTAGE_IMBALANCE
    )
    expected.genConnectStatus.value = to_hex_binary(ConnectStatusType.AVAILABLE | ConnectStatusType.OPERATING)
    expected.storConnectStatus.value = to_hex_binary(ConnectStatusType.TEST | ConnectStatusType.FAULT_ERROR)
    expected.manufacturerStatus.value = "ab-12$"  # Length limit on field
    async with generate_async_session(pg_base_config) as session:
        await DERStatusManager.upsert_der_status_for_site(
            session,
            PUBLIC_SITE_DER_ID,
            scope,
            clone_class_instance(expected),
        )

    # Use a new session to query everything back
    async with generate_async_session(pg_base_config) as session:
        actual = await DERStatusManager.fetch_der_status_for_site(session, PUBLIC_SITE_DER_ID, scope)

        assert_class_instance_equality(
            DERStatus,
            expected,
            actual,
            ignored_properties=set(["href", "subscribable", "type", "readingTime"]),
        )
        assert actual.href.startswith(scope.href_prefix)
        assert str(site_id) in actual.href
        assert_datetime_equal(now, actual.readingTime)  # Should be set to server time

    # Archive records will be written on updates, otherwise they won't be touched
    async with generate_async_session(pg_base_config) as session:
        archive_records = (await session.execute(select(ArchiveSiteDERStatus))).scalars().all()
        if site_id == 1:
            # site_id 1 is the test case that will update an existing record
            assert len(archive_records) == 1
            assert archive_records[0].manufacturer_status == "mnstat", "This is the original value from the DB"
            assert archive_records[0].deleted_time is None
            assert_nowish(archive_records[0].archive_time)
        else:
            assert len(archive_records) == 0

    mock_NotificationManager.notify_changed_deleted_entities.assert_called_once_with(
        SubscriptionResource.SITE_DER_STATUS, now
    )
